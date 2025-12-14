import pandas as pd
import os
from collections import Counter

print("=" * 60)
print("📊 ПРОСМОТР ДАННЫХ 2ГИС - КИРОВСКИЙ РАЙОН")
print("=" * 60)

CSV_PATH = 'data/kirovsky_simple.csv'

if not os.path.exists(CSV_PATH):
    print(f"❌ Файл не найден: {CSV_PATH}")
    print("Сначала создайте CSV файл командой:")
    print("python3 export_simple.py")
    exit(1)

try:
    # Загружаем данные
    df = pd.read_csv(CSV_PATH)
    
    print(f"✅ УСПЕШНО ЗАГРУЖЕНО!")
    print(f"📊 Всего объектов: {len(df)}")
    
    print(f"\n📋 КОЛОНКИ В ДАННЫХ:")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i}. {col}")
    
    print(f"\n👀 ПЕРВЫЕ 5 ОБЪЕКТОВ:")
    print("-" * 50)
    for i in range(min(5, len(df))):
        row = df.iloc[i]
        print(f"\n🏢 ОБЪЕКТ #{i+1}:")
        print(f"  Название: {row.get('name', '—')}")
        print(f"  Адрес: {row.get('address', '—')}")
        if 'rubrics' in df.columns and pd.notna(row.get('rubrics')):
            print(f"  Рубрики: {row.get('rubrics')}")
    
    if 'rubrics' in df.columns:
        print(f"\n��️  СТАТИСТИКА ПО РУБРИКАМ:")
        all_rubrics = []
        for rubrics in df['rubrics'].dropna():
            if isinstance(rubrics, str):
                rubric_list = [r.strip() for r in rubrics.split(',') if r.strip()]
                all_rubrics.extend(rubric_list)
        
        if all_rubrics:
            rubric_counts = Counter(all_rubrics)
            print("Самые частые рубрики:")
            for i, (rubric, count) in enumerate(rubric_counts.most_common(10), 1):
                print(f"  {i}. {rubric}: {count} объектов")
    
    print(f"\n💾 Файл данных: {CSV_PATH}")
    print("📁 Откройте его в Excel или Numbers для детального просмотра")
    
except Exception as e:
    print(f"❌ Ошибка: {e}")
