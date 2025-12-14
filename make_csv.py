import pandas as pd
import os

print("=" * 50)
print("СОЗДАНИЕ CSV ФАЙЛА ИЗ PARQUET")
print("=" * 50)

PARQUET_PATH = 'data/kirovsky_all.parquet'
CSV_PATH = 'data/kirovsky_simple.csv'

if not os.path.exists(PARQUET_PATH):
    print(f"❌ Parquet файл не найден: {PARQUET_PATH}")
    print("Сначала запустите сбор данных: python pull_2gis.py")
    exit(1)

try:
    # Читаем данные
    df = pd.read_parquet(PARQUET_PATH)
    print(f"✅ Загружено {len(df)} объектов")
    
    # Создаем список для CSV
    csv_data = []
    
    print("🔄 Преобразование данных...")
    for index, row in df.iterrows():
        record = {
            'id': row.get('id', ''),
            'name': row.get('name', ''),
            'address': row.get('address_name', ''),
        }
        
        # Координаты
        point = row.get('point', {})
        if isinstance(point, dict):
            record['lon'] = point.get('lon', '')
            record['lat'] = point.get('lat', '')
        else:
            record['lon'] = ''
            record['lat'] = ''
        
        # Рубрики
        rubrics = row.get('rubrics', [])
        if isinstance(rubrics, list):
            rubric_names = []
            for rubric in rubrics:
                if isinstance(rubric, dict):
                    rubric_names.append(rubric.get('name', ''))
            record['rubrics'] = ', '.join(rubric_names)
        else:
            record['rubrics'] = str(rubrics)
        
        csv_data.append(record)
    
    # Сохраняем в CSV
    csv_df = pd.DataFrame(csv_data)
    csv_df.to_csv(CSV_PATH, index=False, encoding='utf-8')
    
    print(f"✅ CSV файл создан: {CSV_PATH}")
    print(f"📊 Сохранено объектов: {len(csv_df)}")
    print(f"📋 Колонки: {list(csv_df.columns)}")
    
    # Показываем пример
    print("\n👀 Пример данных:")
    print(csv_df.head(3).to_string())
    
except Exception as e:
    print(f"❌ Ошибка: {e}")
