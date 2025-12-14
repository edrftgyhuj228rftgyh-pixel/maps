import pandas as pd
import os
import json
from collections import Counter

print("=" * 60)
print("📊 ЭКСПОРТ ДАННЫХ В CSV")
print("=" * 60)

PARQUET_PATH = 'data/kirovsky_all.parquet'
CSV_PATH = 'data/kirovsky_all_data.csv'

def export_to_csv():
    if not os.path.exists(PARQUET_PATH):
        print(f"❌ Файл не найден: {PARQUET_PATH}")
        return
    
    print(f"✅ Файл найден: {PARQUET_PATH}")
    
    try:
        # Читаем Parquet
        df = pd.read_parquet(PARQUET_PATH)
        print(f"📖 Загружено {len(df)} записей")
        
        # Создаем список для CSV данных
        csv_data = []
        
        print("🔄 Обработка данных...")
        for index, row in df.iterrows():
            record = {}
            
            # Базовые поля
            record['id'] = row.get('id', '')
            record['name'] = row.get('name', '')
            record['address_name'] = row.get('address_name', '')
            
            # Обрабатываем координаты
            point = row.get('point', {})
            if isinstance(point, dict):
                record['longitude'] = point.get('lon', '')
                record['latitude'] = point.get('lat', '')
            else:
                record['longitude'] = ''
                record['latitude'] = ''
            
            # Обрабатываем рубрики
            rubrics = row.get('rubrics', [])
            if isinstance(rubrics, list):
                rubric_names = []
                for rubric in rubrics:
                    if isinstance(rubric, dict):
                        rubric_names.append(rubric.get('name', ''))
                record['rubrics'] = ', '.join(rubric_names)
            else:
                record['rubrics'] = str(rubrics)
            
            # Дополнительные поля если есть
            record['type'] = row.get('type', '')
            record['_collected_at'] = row.get('_collected_at', '')
            
            csv_data.append(record)
        
        # Создаем DataFrame для CSV
        csv_df = pd.DataFrame(csv_data)
        
        # Сохраняем в CSV
        csv_df.to_csv(CSV_PATH, index=False, encoding='utf-8')
        
        print(f"✅ Данные экспортированы в: {CSV_PATH}")
        print(f"📝 Всего записей в CSV: {len(csv_df)}")
        
        # Показываем статистику
        print("\n" + "=" * 50)
        print("📈 СТАТИСТИКА ЭКСПОРТА")
        print("=" * 50)
        
        print(f"Всего объектов: {len(csv_df)}")
        print(f"Колонки в CSV: {list(csv_df.columns)}")
        
        # Статистика по рубрикам
        if 'rubrics' in csv_df.columns:
            all_rubrics = []
            for rubrics_str in csv_df['rubrics'].dropna():
                rubrics_list = [r.strip() for r in rubrics_str.split(',') if r.strip()]
                all_rubrics.extend(rubrics_list)
            
            if all_rubrics:
                rubric_counts = Counter(all_rubrics)
                print(f"\n🏷️ Топ-15 рубрик:")
                for i, (rubric, count) in enumerate(rubric_counts.most_common(15), 1):
                    print(f"  {i:2d}. {rubric}: {count} объектов")
        
        # Географическая статистика
        if 'longitude' in csv_df.columns and 'latitude' in csv_df.columns:
            coords_df = csv_df[(csv_df['longitude'] != '') & (csv_df['latitude'] != '')]
            if len(coords_df) > 0:
                print(f"\n🗺️ Геоданные:")
                print(f"  Объектов с координатами: {len(coords_df)}")
                print(f"  Долгота: {coords_df['longitude'].min():.4f} - {coords_df['longitude'].max():.4f}")
                print(f"  Широта: {coords_df['latitude'].min():.4f} - {coords_df['latitude'].max():.4f}")
        
        print("\n" + "=" * 50)
        print("🎉 ЭКСПОРТ ЗАВЕРШЕН!")
        print("=" * 50)
        print("Следующие шаги:")
        print("  1. Откройте файл в Excel/Numbers: data/kirovsky_all_data.csv")
        print("  2. Используйте фильтры для анализа данных")
        print("  3. Для визуализации запустите: python create_map.py")
        
    except Exception as e:
        print(f"❌ Ошибка при экспорте: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    export_to_csv()