import pandas as pd
import pyarrow.parquet as pq
import os
from collections import Counter

print("=" * 60)
print("📊 ПРОСМОТР ДАННЫХ 2ГИС")
print("=" * 60)

PARQUET_PATH = 'data/kirovsky_all.parquet'

def main():
    if not os.path.exists(PARQUET_PATH):
        print(f"❌ Файл не найден: {PARQUET_PATH}")
        return
    
    print(f"✅ Файл найден: {PARQUET_PATH}")
    
    try:
        df = pd.read_parquet(PARQUET_PATH)
        print(f"✅ Успешно загружено {len(df)} записей")
        
        print("\n📋 ПЕРВЫЕ 10 ЗАПИСЕЙ:")
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        print(df.head(10))
        
        print(f"\n📊 ОСНОВНАЯ ИНФОРМАЦИЯ:")
        print(f"Всего записей: {len(df)}")
        print(f"Колонки: {list(df.columns)}")
        
        # Анализ рубрик
        if 'rubrics' in df.columns:
            print(f"\n🏷️ СТАТИСТИКА РУБРИК:")
            all_rubrics = []
            for rubrics in df['rubrics'].dropna():
                if isinstance(rubrics, list):
                    for rubric in rubrics:
                        if isinstance(rubric, dict) and 'name' in rubric:
                            all_rubrics.append(rubric['name'])
            
            if all_rubrics:
                from collections import Counter
                rubric_counts = Counter(all_rubrics)
                print("Топ-10 рубрик:")
                for rubric, count in rubric_counts.most_common(10):
                    print(f"  {rubric}: {count}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    main()