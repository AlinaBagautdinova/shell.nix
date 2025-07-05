import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from pathlib import Path

# Настройки
RANDOM_SEED = 42
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

NUM_CASES = 35000
# Вероятность того, что по делу в итоге будет возбуждено ИП
PROB_ENFORCEMENT_EXISTS = 0.92

# Настройки 'длинного хвоста'
# Вероятность того, что успешное дело попадет в 'длинный хвост'
LONG_TAIL_PROBABILITY = 0.06
# Диапазон задержки для 'длинного хвоста' в месяцах (от 5 до 12)
LONG_TAIL_MONTHS_RANGE = (5, 12) 

# Настройки периода и сезонности 
# Начинаем генерировать ИЛ с конца 2023, чтобы 'наполнить' начало 2024 событиями ВИП
GENERATION_START_DATE = datetime(2023, 10, 1) 
GENERATION_END_DATE = datetime(2024, 12, 29)
# Коэффициент сезонности: насколько 4-й квартал 2024 'активнее' остальных
Q4_2024_WEIGHT = 1.2

# Справочники
EMPLOYEES = [
    'Иванов И.И.', 'Петров П.П.', 'Сидорова А.А.', 'Кузнецов В.В.', 
    'Смирнова О.Н.', 'Васильев Д.С.', 'Михайлова Е.Г.', 'Новиков А.В.'
]

ACTIVITY_TYPES = {
    'court_prep': 'Подача искового заявления',
    'court_decision': ['Назначено судебное заседание', 'Вынесено судебное решение','Судебное решение с опечаткой','Получено судебное решение'],
    'writ_of_execution': 'Получен исполнительный лист',
    'fssp_send': 'Исполнительный лист направлен в ФССП',
    'fssp_return': 'ИЛ возвращен на исправление',
    'enforcement_proceedings': 'Возбуждено исполнительное производство',
    'other': ['Отправлено уведомление должнику', 'Платеж от должника', 'Дело закрыто (оплата)']
}


# Логика генерации данных

# Создаем список всех возможных дней для генерации ИЛ (исполнительного листа) и их веса для сезонности
all_possible_days = pd.to_datetime(pd.date_range(start=GENERATION_START_DATE, end=GENERATION_END_DATE, freq='D'))
day_weights = [Q4_2024_WEIGHT if (date.year == 2024 and date.quarter == 4) else 1.0 for date in all_possible_days]

# Генерируем все даты получения ИЛ за один раз, с учетом весов
generated_il_dates = random.choices(all_possible_days, weights=day_weights, k=NUM_CASES)
all_activities = []

for case_id in range(1, NUM_CASES + 1):
    case_events = []
    # Берем заранее сгенерированную дату ИЛ
    cohort_start_date = generated_il_dates[case_id - 1]
    
    # Добавляем событие получения ИЛ
    case_events.append({'type': ACTIVITY_TYPES['writ_of_execution'], 'date': cohort_start_date})

    # Решаем, будет ли ВИП, и генерируем его дату
    if np.random.random() < PROB_ENFORCEMENT_EXISTS:
        # Логика генерации задержки с 'длинным хвостом'
        if random.random() > LONG_TAIL_PROBABILITY:
            # 92% успешных дел попадают в 'основной поток'
            month_choice = np.random.choice([1, 2, 3, 4], p=[0.10/0.9, 0.50/0.9, 0.20/0.9, 0.10/0.9])
            delay_days = random.randint((month_choice - 1) * 30 + 10, month_choice * 30)
        else:
            # 6% успешных дел попадают в 'длинный хвост'
            min_days = (LONG_TAIL_MONTHS_RANGE[0] - 1) * 30 + 1
            max_days = LONG_TAIL_MONTHS_RANGE[1] * 30
            delay_days = random.randint(min_days, max_days)

        enforcement_date = cohort_start_date + timedelta(days=delay_days)
        case_events.append({'type': ACTIVITY_TYPES['enforcement_proceedings'], 'date': enforcement_date})

    # Генерируем остальные события относительно даты получения ИЛ
    fssp_send_date = cohort_start_date + timedelta(days=random.randint(1, 7))
    date_court_decision_received = cohort_start_date - timedelta(days=random.randint(7, 21))
    date_court_decision_made = date_court_decision_received - timedelta(days=random.randint(15, 45))
    date_meeting_scheduled = date_court_decision_made - timedelta(days=random.randint(20, 60))
    date_claim_filed = date_meeting_scheduled - timedelta(days=random.randint(1, 5))

    case_events.extend([
        {'type': ACTIVITY_TYPES['fssp_send'], 'date': fssp_send_date},
        {'type': ACTIVITY_TYPES['court_prep'], 'date': date_claim_filed},
        {'type': ACTIVITY_TYPES['court_decision'][0], 'date': date_meeting_scheduled},
        {'type': ACTIVITY_TYPES['court_decision'][1], 'date': date_court_decision_made},
        {'type': ACTIVITY_TYPES['court_decision'][3], 'date': date_court_decision_received},
    ])
    
    # Добавляем все события по делу в общий список, присваивая сотрудника
    for event in case_events:
        all_activities.append({
            'case_id': case_id,
            'activity_type': event['type'],
            'activity_date': event['date'].date(),
            'employee': random.choice(EMPLOYEES)
        })

# Сохранение
df = pd.DataFrame(all_activities)
df_shuffled = df.sample(frac=1, random_state=RANDOM_SEED).reset_index(drop=True)

project_root = Path(__file__).parent.parent
output_path = project_root / 'data' / 'activities.csv'
output_path.parent.mkdir(parents=True, exist_ok=True)
df_shuffled.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f'Генерация данных завершена. Файл сохранен по пути: {output_path}')