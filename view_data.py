import pandas as pd
import os
from collections import Counter

print("🎯" * 60)
print("📊 ПРОСМОТР ДАННЫХ 2ГИС - КИРОВСКИЙ РАЙОН СПБ")
print("🎯" * 60)

CSV_PATH = 'data/kirovsky_simple.csv'

def print_section(title):
    print()
    print("─" * 60)
    print(f"📌 {title}")
    print("─" * 60)

try:
    # Проверяем файл
    if not os.path.exists(CSV_PATH):
        print(f"❌ Файл не найден: {CSV_PATH}")
        print("\nСоздайте CSV файл командой:")
        print("python make_csv.py")
        exit(1)
    
    # Загружаем данные
    df = pd.read_csv(CSV_PATH)
    
    print_section("📈 ОБЩАЯ СТАТИСТИКА")
    print(f"✅ Успешно загружено: {len(df)} объектов")
    print(f"📋 Колонки в данных: {', '.join(df.columns)}")
    
    print_section("👀 ПРЕДПРОСМОТР ДАННЫХ")
    print("Первые 8 объектов:")
    print()
    
    # Настройки отображения
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_colwidth', 40)
    
    # Показываем первые записи
    print(df.head(8).to_string())
    
    print_section("🏷️ АНАЛИЗ РУБРИК")
    if 'rubrics' in df.columns:
        # Собираем статистику по рубрикам
        all_rubrics = []
        for rubrics in df['rubrics'].dropna():
            if isinstance(rubrics, str):
                rubric_list = [r.strip() for r in rubrics.split(',') if r.strip()]
                all_rubrics.extend(rubric_list)
        
        if all_rubrics:
            rubric_counts = Counter(all_rubrics)
            total_rubrics = len(all_rubrics)
            
            print(f"Всего упоминаний рубрик: {total_rubrics}")
            print(f"Уникальных рубрик: {len(rubric_counts)}")
            
            print("\nТОП-15 самых частых рубрик:")
            for i, (rubric, count) in enumerate(rubric_counts.most_common(15), 1):
                print(f"  {i:2d}. {rubric:25} — {count:3} объектов")
        else:
            print("❌ Рубрики не найдены в данных")
    else:
        print("❌ Колонка 'rubrics' отсутствует")
    
    print_section("🗺️ ГЕОГРАФИЧЕСКИЕ ДАННЫЕ")
    if 'lon' in df.columns and 'lat' in df.columns:
        # Фильтруем объекты с координатами
        coords_df = df[(df['lon'] != '') & (df['lat'] != '')].copy()
        
        # Преобразуем в числа
        coords_df['lon'] = pd.to_numeric(coords_df['lon'], errors='coerce')
        coords_df['lat'] = pd.to_numeric(coords_df['lat'], errors='coerce')
        coords_df = coords_df.dropna(subset=['lon', 'lat'])
        
        if len(coords_df) > 0:
            print(f"✅ Объектов с координатами: {len(coords_df)}")
            print(f"📍 Географический охват:")
            print(f"   • Долгота: {coords_df['lon'].min():.4f} — {coords_df['lon'].max():.4f}")
            print(f"   • Широта:  {coords_df['lat'].min():.4f} — {coords_df['lat'].max():.4f}")
            print(f"   • Центр:   {coords_df['lon'].mean():.4f}, {coords_df['lat'].mean():.4f}")
        else:
            print("❌ Нет объектов с координатами")
    else:
        print("❌ Колонки с координатами отсутствуют")
    
    print_section("🎯 РЕКОМЕНДАЦИИ")
    print("💡 СОВЕТЫ:")
    print("   1. 📁 Откройте файл в Excel или Numbers:")
    print(f"      {CSV_PATH}")
    print("   2. 🔍 Используйте фильтры для анализа")
    print("   3. 🗺️ Для карты запустите: python create_map.py")
    print("   4. 📊 Для статистики: python analyze_data.py")
    
    print()
    print("✅ Анализ завершен успешно!")
    
except Exception as e:
    print(f"❌ Ошибка: {e}")
    import traceback
    traceback.print_exc()
