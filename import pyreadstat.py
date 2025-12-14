import pyreadstat
import pandas as pd

# 1. Прочитать SPSS-файл
FILE = "/Users/romannovgorodov/Downloads/Б†І†_О≠Ђ†©≠-ѓЃ™гѓ™® (21.09).sav"  # <-- тут просто укажи свой файл
df, meta = pyreadstat.read_sav(FILE)

# Посмотреть, что вообще есть
print("Имена переменных:")
print(meta.column_names)

print("\nМетки вопросов (labels):")
for name, label in zip(meta.column_names, meta.column_labels):
    print(f"{name}: {label}")
