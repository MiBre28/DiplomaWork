import os
import pyodbc
import pandas as pd
import fdb
import re
import datetime as dt
import openpyxl

con = fdb.connect(dsn='C:/bazy/test.fdb', user='SYSDBA', password='masterkey')
#polaczenie z baza danych

# Funkcja do sprawdzenia poprawności formatu daty
def is_valid_date(date):
    return bool(re.match(r'^\d{2}\.\d{2}\.\d{4}$', date))

# Pytaj użytkownika o liczbę dni
while True:
    try:
        num_days = int(input("Podaj liczbę dni z których pobrać zlecenia: "))

        if 1 <= num_days <= 7:
            break  # Wyjście z pętli, jeśli wprowadzona wartość jest poprawna
        else:
            print("Proszę podać liczbę z przedziału od 1 do 7.")
    except ValueError:  # Jeśli wprowadzona wartość nie jest liczbą całkowitą
        print("Proszę podać poprawną liczbę.")

dates = []

# Pytaj użytkownika o daty, dopóki wszystkie podane daty nie będą poprawne
while len(dates) < num_days:
    date = input(f"Podaj datę (np. 04.07.2022) dla dnia {len(dates) + 1}: ")

    if is_valid_date(date) and date not in dates:
        dates.append(date)
    elif date in dates:
        print("Data już została podana. Nie możesz jej powtarzać.")
    else:
        print("Niepoprawny format daty. Spróbuj ponownie.")

# Przygotuj fragment zapytania SQL dla każdej daty
date_conditions = [f"limit.limit_data_prod = '{date} 00:00'" for date in dates]

# Łącz warunki dat w jedno zapytanie SQL przy użyciu operatora OR
sql_condition = " OR ".join(date_conditions)

cur = con.cursor()
nor = con.cursor()
cur.execute(f"select zamowienia_spec.zamowienia_spec_id as numer_zamowienia,wg.artykul_kod as MODEL_KOD,wg.nazwa1 as model,zamowienia_spec.zebrana_nazwa as tkanina,limit_spec.ilosc_plan as ilosc from przegzlecprod_spec inner join limit_spec on przegzlecprod_spec.limit_spec_id = limit_spec.limit_spec_id inner join przegzlecprod on (przegzlecprod.przegzlecprod_id = przegzlecprod_spec.przegzlecprod_id) inner join zamowienia_spec on limit_spec.zamowienia_spec_id = zamowienia_spec.zamowienia_spec_id inner join zamowienia on (zamowienia_spec.zamowienia_id = zamowienia.zamowienia_id) inner join artykul wg on (zamowienia_spec.artykul_id = wg.artykul_id) inner join klienci on zamowienia.klienci_id = klienci.klienci_id inner join limit on limit_spec.limit_id = limit.limit_id inner join lc_wydzialy on przegzlecprod.lc_wydzialy_id = lc_wydzialy.lc_wydzialy_id inner join jednostki on jednostki.jednostki_id = wg.jednostki_id where ({sql_condition}) and lc_wydzialy.lc_wydzialy_opis = 'KROJOWNIA'")
nor.execute("select RTRIM(ARTYKUL.ARTYKUL_KOD) as MODEL_KOD, RTRIM(ARTYKUL.NAZWA1) as MODEL, NORMA_TK_NEW.ILOSC as METRY from NORMA_TK_NEW inner join ARTYKUL on ARTYKUL.ARTYKUL_ID = NORMA_TK_NEW.ARTYKUL_ID")
#wykonywanie zapytania sql

normy = nor.fetchall()
zlecenia = cur.fetchall()
#ladowanie danych

normy = pd.DataFrame(normy)
zlecenia = pd.DataFrame(zlecenia)
#tworzenie plikow dataframe

#normy.to_excel("C:/Users/breczko/Desktop/MB/normy.xlsx", sheet_name='normy')
#zlecenia.to_excel("C:/Users/breczko/Desktop/MB/zlecenia.xlsx", sheet_name='zamowienia')
#zapis pikow

normy[0] = normy[0].str.replace(' ', '')
zlecenia[1] = zlecenia[1].str.replace(' ', '')
#usuwanie spacji - przygotywywanie danych

normy = normy.sort_values(by=[0, 2], ascending=[True, False])
normy = normy.drop_duplicates(subset=[0], keep='first')
#usuwanie duplikatów, które wynikają z różnych kombinacji poduszek -
#- informacja nie do zweryfikwoania w uwagach bez wpływu na algorytm optymalizacji

zlecenia.loc[zlecenia[2].str.contains('APR'), 1] = \
    zlecenia.loc[zlecenia[2].str.contains('APR'), 1].str.replace('A$', '', regex=True)
#pomijanie apretowania
filtered_df = zlecenia[~zlecenia[1].str.startswith('SERW')]
#pomijamy pliki SERW


#pd.set_option('display.max_columns', None)
#pokazywanie wszystkich kolumn
grouped = filtered_df.groupby(1)
#grupujemy dane
results = []

for index, group in grouped:
    group_sorted = group.sort_values(by=4, ascending=False)

    current_rows = []
    current_sum = 0

    for _, row in group_sorted.iterrows():

        if current_sum + row[4] > 20:
            remaining = row[4]

            while remaining > 0:
                amount_to_take = min(20 - current_sum, remaining)
                current_sum += amount_to_take
                remaining -= amount_to_take

                details = f"Nr.sys:{row[0]} Ilość={amount_to_take} Model:{row[2]} Tkanina:{row[3]}"
                current_rows.append(details)

                if current_sum == 20 or _ == group_sorted.index[-1]:
                    results.append({
                        'KodModelu': index,
                        'SumaWarstw': current_sum,
                        'Details': "; ".join(current_rows)
                    })

                    current_rows = []
                    current_sum = 0
        else:
            current_sum += row[4]
            details = f"Nr.sys:{row[0]} Ilość={row[4]} Model:{row[2]} Tkanina:{row[3]}"
            current_rows.append(details)

    if current_rows:
        results.append({
            'KodModelu': index,
            'SumaWarstw': current_sum,
            'Details': "; ".join(current_rows)
        })

results_df = pd.DataFrame(results)
# Podział sum na bloki po 20

# 1. Utwórz słownik z normy_df
normy_dict = pd.Series(normy[2].values, index=normy[0]).to_dict()

# 2. Dla każdego wiersza w `results_df` przypisz odpowiednią wartość z normy_dict do nowej kolumny
results_df['MetryTkaniny'] = results_df['KodModelu'].map(normy_dict)

results_df = results_df.dropna(subset=['MetryTkaniny'])
# pomnijanie wzorów

def compute_time_for_topspin(sumawarstw, metrytkanin):
    return (metrytkanin * sumawarstw) / 0.3

def compute_time_for_cutter(sumawarstw, metrytkanin):
    # obliczanie mnoznika
    mnoznik = 1 + 0.05 * (sumawarstw - 1)
    return (metrytkanin * mnoznik) / 0.185

# Obliczanie czasów dla każdego wiersza w dataframe
results_df['CzasTopSpin'] = results_df.apply\
    (lambda row: compute_time_for_topspin(row['SumaWarstw'], row['MetryTkaniny']), axis=1)
results_df['CzasCutter'] = results_df.apply\
    (lambda row: compute_time_for_cutter(row['SumaWarstw'], row['MetryTkaniny']), axis=1)

def get_machine_hours(prompt):
    while True:
        try:
            time_input = input(prompt)

            # Sprawdzanie, czy użytkownik wprowadził dane w formacie godzina:minuta
            if ':' in time_input:
                hours, minutes = map(int, time_input.split(":"))
                if 0 <= hours <= 167 and 0 <= minutes <= 59:
                    total_minutes = hours * 60 + minutes
                    return total_minutes
                else:
                    raise ValueError
            # Sprawdzanie, czy użytkownik wprowadził dane tylko w formie godzin
            else:
                hours = int(time_input)
                if 0 <= hours <= 168:
                    return hours * 60
                else:
                    raise ValueError

        except ValueError:
            print("Niepoprawny format. Wprowadź ilość godzin (np. 5 lub 5:30).")

#pierwsze topspiny drugie cuttery

# Dla maszyn typu topspin
num_machines_t = int(input("Ile maszyn typu topspin jest dostępnych? "))
machines_data_t = []

for i in range(1, num_machines_t + 1):
    available_minutes = get_machine_hours(
        f"Dla maszyny typu topspin numer {i}, podaj dostępne godziny pracy (np. 5 lub 5:30): ")
    machines_data_t.append({
        "machine_number": i,
        "available_minutes": available_minutes
    })

# Dla maszyn typu cutter
num_machines_k = int(input("Ile maszyn typu cutter jest dostępnych? "))
machines_data_k = []

for i in range(1, num_machines_k + 1):
    available_minutes = get_machine_hours(
        f"Dla maszyny typu cutter numer {i}, podaj dostępne godziny pracy (np. 5 lub 5:30): ")
    machines_data_k.append({
        "machine_number": i,
        "available_minutes": available_minutes
    })

# print(f"Liczba zgrupowanych zadań: {len(results_df)}")

def assign_tasks_to_machine(machine_data, tasks, machine_type):
    if machine_data['available_minutes'] <= 0:
        return tasks

    tasks_for_machine = []
    machine_time = machine_data['available_minutes']
    to_remove = []

    for index, task in tasks.iterrows():
        task_time = task['CzasCutter'] if machine_type == 'Cutter' else task['CzasTopSpin']

        if machine_time >= task_time:
            tasks_for_machine.append(task)
            machine_time -= task_time
            to_remove.append(index)

    machine_data['available_minutes'] = machine_time
    # Dodajemy nowe zadania do istniejącej listy zadań maszyny
    if 'assigned_tasks' in machine_data:
        machine_data['assigned_tasks'].extend(tasks_for_machine)
    else:
        machine_data['assigned_tasks'] = tasks_for_machine

    tasks.drop(index=to_remove, inplace=True)

    # Jeśli przypisujesz zadania do maszyn typu cutter i nie ma już zadań w tasks_k
    # , zacznij brać zadania z tasks_t
    if machine_type == 'Cutter' and tasks.empty:
        return tasks_t
    # # Jeśli przypisujesz zadania do maszyn typu topspin i nie ma już zadań w tasks_t,
    # zacznij brać zadania z tasks_k
    # elif machine_type == 't' and tasks.empty:
    #     return tasks_k

    return tasks

# Sortowanie zadań
tasks_t = results_df[results_df['SumaWarstw'] == 1].copy()
tasks_k = results_df[results_df['SumaWarstw'] > 1].sort_values(by='SumaWarstw', ascending=False).copy()

# print(f"Liczba zadań w tasks_k po przydzieleniu dla maszyn typu cutter: {len(tasks_k)}")
# print(f"Liczba zadań w tasks_t po przydzieleniu dla maszyn typu topspin: {len(tasks_t)}")

# Przydzielanie dla maszyn typu topspin (tylko z sumawarstw = 1)
for machine in machines_data_t:
    tasks_t = assign_tasks_to_machine(machine, tasks_t, 'TopSpin')

# Przydzielanie dla maszyn typu cutter
for machine in machines_data_k:
    tasks_k = assign_tasks_to_machine(machine, tasks_k, 'Cutter')

if tasks_t.empty:
    tasks_k = tasks_k.sort_values(by='SumaWarstw')
    for machine in machines_data_t:
        tasks_k = assign_tasks_to_machine(machine, tasks_k, 'Topspin')

# print(f"Liczba zadań w tasks_k po przydzieleniu dla maszyn typu cutter: {len(tasks_k)}")
# print(f"Liczba zadań w tasks_t po przydzieleniu dla maszyn typu topspin: {len(tasks_t)}")

# Zapisywanie do plików Excel
with pd.ExcelWriter("C:/Users/breczko/Desktop/MB/raport.xlsx") as writer:
    for machine in machines_data_k + machines_data_t:
        machine_number = machine['machine_number']
        machine_type = 'Cutter' if machine in machines_data_k else 'TopSpin'
        task_time_column = 'CzasCutter' if machine_type == 'Cutter' else 'CzasTopSpin'

        machine_tasks = machine.get('assigned_tasks', [])  # Pobieramy przypisane zadania dla maszyny
        if machine_tasks:
            machine_tasks_df = pd.DataFrame(machine_tasks)
            machine_tasks_df = machine_tasks_df[
                ['KodModelu', 'SumaWarstw', 'Details', 'MetryTkaniny', task_time_column]]

            # Mnożymy metrytkę tkaniny przez sumę warstw
            machine_tasks_df['MetryTkaniny'] *= machine_tasks_df['SumaWarstw']

            # Dodajemy wiersz sumujący dla kolumn 'metrytkaniny' i 'Czas realizacji'
            sum_row = pd.DataFrame({
                'KodModelu': ['Suma'],
                'MetryTkaniny': [machine_tasks_df['MetryTkaniny'].sum()],
                task_time_column: [machine_tasks_df[task_time_column].sum()]
            })
            machine_tasks_df = pd.concat([machine_tasks_df, sum_row], ignore_index=True)

            machine_tasks_df.rename(columns={task_time_column: 'CzasRealizacji(min)'}, inplace=True)
            machine_tasks_df.to_excel(writer, sheet_name=f"Maszyna_{machine_type}_{machine_number}",
                                      index=False)

    # Zapis nieprzypisanych zleceń
    unassigned_tasks = pd.concat([tasks_k, tasks_t])
    # print(f"Liczba nieprzypisanych zadań przed zapisaniem do Excela: {len(unassigned_tasks)}")
    if not unassigned_tasks.empty:
        unassigned_tasks[['KodModelu', 'SumaWarstw', 'Details', 'MetryTkaniny']].to_excel\
            (writer, sheet_name="Nieprzypisane_zlecenia", index=False)

print('Gratulacje! Pliki znajdziesz tutaj: C:/Users/breczko/Desktop/MB/raport.xlsx')

# normy.to_excel("C:/Users/breczko/Desktop/MB/normy1.xlsx", sheet_name='normy')
# zlecenia.to_excel("C:/Users/breczko/Desktop/MB/zlecenia1.xlsx", sheet_name='zamowienia')
# results_df.to_excel("C:/Users/breczko/Desktop/MB/testresults.xlsx", sheet_name='wyniki', engine='openpyxl')

# # Ustawienie opcji, aby wyświetlić wszystkie wiersze
# pd.set_option('display.max_rows', None)
#
# # Ustawienie opcji, aby wyświetlić wszystkie kolumny
# pd.set_option('display.max_columns', None)
#
# # Ustawienie opcji, aby wyświetlić pełną zawartość komórki
# pd.set_option('display.width', None)
# pd.set_option('display.max_colwidth', None)
#
# print(results_df)