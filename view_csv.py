import pandas as pd

print("=" * 60)
print("📊 ПРОСМОТР CSV ДАННЫХ")
print("=" * 60)

CSV_PATH = 'data/kirovsky_simple.csv'

try:
    df = pd.read_csv(CSV_PATH)
    print(f"✅ Загружено {len(df)} записей")
    
    print(f"\n📋 ПЕРВЫЕ 15 ЗАПИСЕЙ:")
    print("=" * 50)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_colwidth', 30)
    print(df.head(15))
    
    print(f"\n📊 СТАТИСТИКА:")
    print("=" * 50)
    print(f"Всего записей: {len(df)}")
    print(f"Колонки: {list(df.columns)}")
    
    print(f"\n🔍 ИНФОРМАЦИЯ О ДАННЫХ:")
    print("=" * 50)
    print(df.info())
    
    # Анализ рубрик
    if 'rubrics' in df.columns:
        print(f"\n🏷️ СТАТИСТИКА ПО РУБРИКАМ:")
        print("=" * 50)
        from collections import Counter
        all_rubrics = []
        for rubrics in df['rubrics'].dropna():
            if isinstance(rubrics, str):
                rubric_list = [r.strip() for r in rubrics.split(',') if r.strip()]
                all_rubrics.extend(rubric_list)
        
        if all_rubrics:
            rubric_counts = Counter(all_rubrics)
            print("Топ-15 самых частых рубрик:")
            for i, (rubric, count) in enumerate(rubric_counts.most_common(15), 1):
                print(f"  {i:2d}. {rubric}: {count} объектов")
    
    # Статистика по координатам
    if 'lon' in df.columns and 'lat' in df.columns:
        coords_df = df[(df['lon'] != '') & (df['lat'] != '')]
        print(f"\n🗺️ ГЕОГРАФИЧЕСКИЕ ДАННЫЕ:")
        print("=" * 50)
        print(f"Объектов с координатами: {len(coords_df)}")
        if len(coords_df) > 0:
            print(f"Долгота: {coords_df['lon'].min()} - {coords_df['lon'].max()}")
            print(f"Широта: {coords_df['lat'].min()} - {coords_df['lat'].max()}")
    
    print(f"\n💾 Файл: {CSV_PATH}")
    print("📁 Откройте его в Excel, Numbers или любом другом редакторе таблиц")
    
except Exception as e:
    print(f"❌ Ошибка: {e}")
