import pandas as pd
import os
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
        df = pd.read_parquet(PARQUET_PATH)
        print(f"📖 Загружено {len(df)} записей")
        
        csv_data = []
        
        print("🔄 Обработка данных...")
        for index, row in df.iterrows():
            record = {}
            record['id'] = row.get('id', '')
            record['name'] = row.get('name', '')
            record['address_name'] = row.get('address_name', '')
            
            point = row.get('point', {})
            if isinstance(point, dict):
                record['longitude'] = point.get('lon', '')
                record['latitude'] = point.get('lat', '')
            else:
                record['longitude'] = ''
                record['latitude'] = ''
            
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
        
        csv_df = pd.DataFrame(csv_data)
        csv_df.to_csv(CSV_PATH, index=False, encoding='utf-8')
        
        print(f"✅ Данные экспортированы в: {CSV_PATH}")
        print(f"📝 Всего записей в CSV: {len(csv_df)}")
        
        print("\n📈 СТАТИСТИКА:")
        print(f"Всего объектов: {len(csv_df)}")
        print(f"Колонки: {list(csv_df.columns)}")
        
        if 'rubrics' in csv_df.columns:
            all_rubrics = []
            for rubrics_str in csv_df['rubrics'].dropna():
                rubrics_list = [r.strip() for r in rubrics_str.split(',') if r.strip()]
                all_rubrics.extend(rubrics_list)
            
            if all_rubrics:
                rubric_counts = Counter(all_rubrics)
                print(f"\n🏷️ Топ-10 рубрик:")
                for i, (rubric, count) in enumerate(rubric_counts.most_common(10), 1):
                    print(f"  {i}. {rubric}: {count}")
        
        print(f"\n💾 Файл готов: {CSV_PATH}")
        print("📋 Откройте его в Excel или Numbers")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    export_to_csv()
