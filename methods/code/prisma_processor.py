#!/usr/bin/env python3
"""
PRISMA Processing Script
Обработка собранных данных по методологии PRISMA

Этапы:
1. Загрузка всех файлов с метками источников
2. Дедупликация по URL
3. Отбор по релевантности
4. Оценка ИКМ (Индекс Качества Материала)
5. Создание итоговых таблиц
"""

import json
import csv
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict

BASE_DIR = Path(__file__).parent.parent
RAW_DIR = BASE_DIR / "data" / "prisma_raw"
PROCESSED_DIR = BASE_DIR / "data" / "prisma_processed"
TABLES_DIR = BASE_DIR / "data" / "tables_prisma"

# Создаём директории
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
TABLES_DIR.mkdir(parents=True, exist_ok=True)

# Ключевые слова для релевантности (расширенные)
REQUIRED_KEYWORDS = [
    "медиа", "media", "решени", "decision",
    "аналитик", "analytics", "данн", "data",
    "информаци", "information", "систем", "system",
    "технолог", "technology", "цифров", "digital"
]

OPTIONAL_KEYWORDS = [
    "мониторинг", "monitoring", "приняти", "making",
    "управлен", "management", "бизнес", "business",
    "стратеги", "strategy", "автоматиз", "automat",
    "платформ", "platform", "инструмент", "tool",
    "метод", "method", "модель", "model",
    "прогноз", "predict", "оптимиз", "optim"
]

# Академические домены
ACADEMIC_DOMAINS = [
    "sciencedirect.com", "springer.com", "wiley.com", "nature.com",
    "frontiersin.org", "mdpi.com", "tandfonline.com", "researchgate.net",
    "pmc.ncbi.nlm.nih.gov", "cyberleninka.ru", "elibrary.ru",
    "scholar.google", "semanticscholar.org", "dl.acm.org",
    "ieee.org", "arxiv.org"
]

# Отраслевые домены
INDUSTRY_DOMAINS = [
    "mckinsey.com", "gartner.com", "deloitte.com", "accenture.com",
    "pwc.com", "kpmg.com", "ibm.com", "microsoft.com", "google.com",
    "harvard.edu", "hbr.org", "forbes.com", "bloomberg.com"
]

def load_all_files():
    """Загружает все JSON файлы из prisma_raw"""
    all_materials = []
    
    for json_file in RAW_DIR.glob("*.json"):
        # Парсим имя файла: sector_source_num.json
        parts = json_file.stem.split("_")
        if len(parts) >= 2:
            sector = parts[0]
            source = parts[1]  # yandex, google, scholar, elibrary
        else:
            sector = "unknown"
            source = "unknown"
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            if isinstance(data, list):
                for item in data:
                    item["_sector"] = sector
                    item["_source"] = source
                    item["_file"] = json_file.name
                all_materials.extend(data)
            elif isinstance(data, dict):
                data["_sector"] = sector
                data["_source"] = source
                data["_file"] = json_file.name
                all_materials.append(data)
                
        except Exception as e:
            print(f"Error loading {json_file}: {e}")
    
    return all_materials

def deduplicate(materials):
    """Дедупликация по URL"""
    seen_urls = set()
    unique = []
    duplicates = []
    
    for m in materials:
        url = m.get("url", "")
        # Нормализуем URL
        url_normalized = url.lower().rstrip("/")
        
        if url_normalized not in seen_urls:
            seen_urls.add(url_normalized)
            unique.append(m)
        else:
            duplicates.append(m)
    
    return unique, duplicates

def check_relevance(material):
    """Проверка релевантности по ключевым словам (расширенная)"""
    text = f"{material.get('name', '')} {material.get('snippet', '')}".lower()
    
    # Проверяем обязательные ключевые слова (хотя бы одно)
    has_required = any(kw.lower() in text for kw in REQUIRED_KEYWORDS)
    
    # Считаем опциональные
    optional_count = sum(1 for kw in OPTIONAL_KEYWORDS if kw.lower() in text)
    
    # Более мягкий критерий: достаточно EITHER required OR optional
    return has_required or optional_count >= 1

def filter_relevance(materials):
    """Фильтрация по релевантности"""
    relevant = []
    irrelevant = []
    
    for m in materials:
        if check_relevance(m):
            relevant.append(m)
        else:
            irrelevant.append(m)
    
    return relevant, irrelevant

def calculate_ikm(material):
    """Расчёт Индекса Качества Материала"""
    
    # P1: Релевантность
    text = f"{material.get('name', '')} {material.get('snippet', '')}".lower()
    keyword_count = sum(1 for kw in REQUIRED_KEYWORDS + OPTIONAL_KEYWORDS if kw.lower() in text)
    p1 = min(10, 5 + keyword_count)
    
    # P2: Научность (по домену)
    host = material.get("host_name", "").lower()
    p2 = 5  # базовое значение
    
    if any(domain in host for domain in ACADEMIC_DOMAINS):
        p2 = 9
    elif any(domain in host for domain in INDUSTRY_DOMAINS):
        p2 = 7
    elif "medium.com" in host or "linkedin.com" in host:
        p2 = 5
    
    # P3: Актуальность (по году в тексте)
    year_match = re.search(r'\b(202[0-9])\b', text)
    if year_match:
        year = int(year_match.group(1))
        if year >= 2025:
            p3 = 10
        elif year >= 2024:
            p3 = 9
        elif year >= 2023:
            p3 = 8
        elif year >= 2022:
            p3 = 7
        else:
            p3 = 5
    else:
        p3 = 6
    
    # P4: Достоверность (по домену)
    p4 = p2  # аналогично научности
    
    # P5: Практическая ценность
    practical_keywords = ["стратеги", "strategy", "внедрен", "implementation", 
                         "практик", "practice", "метод", "method"]
    practical_count = sum(1 for kw in practical_keywords if kw in text)
    p5 = min(10, 5 + practical_count)
    
    # ИКМ
    ikm = round((p1 + p2 + p3 + p4 + p5) / 5, 1)
    
    # Тип источника
    if any(domain in host for domain in ACADEMIC_DOMAINS):
        source_type = "academic"
    elif any(domain in host for domain in INDUSTRY_DOMAINS):
        source_type = "industry"
    elif "medium.com" in host or "linkedin.com" in host or "forbes" in host:
        source_type = "media"
    else:
        source_type = "blog"
    
    # DOI
    material_url = material.get("url", "")
    has_doi = "doi" in material_url.lower() or "doi" in text.lower()
    
    return {
        "P1_relevance": p1,
        "P2_scientific": p2,
        "P3_currency": p3,
        "P4_credibility": p4,
        "P5_practical": p5,
        "IKM": ikm,
        "source_type": source_type,
        "has_doi": has_doi,
        "quality_class": "высокий" if ikm >= 8 else ("средний" if ikm >= 6 else "низкий")
    }

def process_sector(materials, sector):
    """Обработка одной сферы"""
    
    # Сортируем по источнику и позиции
    materials_sorted = sorted(materials, key=lambda x: (x.get("_source", ""), x.get("rank", 0)))
    
    # Добавляем ИКМ
    for m in materials_sorted:
        m["assessment"] = calculate_ikm(m)
    
    # Статистика
    stats = {
        "sector": sector,
        "total": len(materials_sorted),
        "by_source": defaultdict(int),
        "by_quality": {"высокий": 0, "средний": 0, "низкий": 0},
        "by_type": defaultdict(int)
    }
    
    for m in materials_sorted:
        stats["by_source"][m["_source"]] += 1
        stats["by_quality"][m["assessment"]["quality_class"]] += 1
        stats["by_type"][m["assessment"]["source_type"]] += 1
    
    return materials_sorted, stats

def save_to_csv(materials, sector):
    """Сохранение в CSV"""
    output_file = TABLES_DIR / f"{sector}_prisma.csv"
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';')
        
        # Заголовок
        writer.writerow([
            "ID", "URL", "Источник_сбора", "Поисковый_запрос_тип",
            "Позиция_в_выдаче", "Заголовок", "Домен",
            "P1_Релевантность", "P2_Научность", "P3_Актуальность",
            "P4_Достоверность", "P5_Практическая_ценность",
            "ИКМ", "Класс_качества", "Тип_источника", "DOI", "Год"
        ])
        
        for i, m in enumerate(materials):
            a = m.get("assessment", {})
            
            # Извлекаем год
            text = f"{m.get('name', '')} {m.get('snippet', '')}"
            year_match = re.search(r'\b(202[0-9])\b', text)
            year = year_match.group(1) if year_match else "2024"
            
            writer.writerow([
                f"{sector.upper()}_{i+1:03d}",
                m.get("url", ""),
                m.get("_source", ""),
                m.get("_file", "").replace(".json", ""),
                m.get("rank", 0) + 1,
                m.get("name", "")[:100],
                m.get("host_name", ""),
                a.get("P1_relevance", 0),
                a.get("P2_scientific", 0),
                a.get("P3_currency", 0),
                a.get("P4_credibility", 0),
                a.get("P5_practical", 0),
                a.get("IKM", 0),
                a.get("quality_class", ""),
                a.get("source_type", ""),
                "Да" if a.get("has_doi") else "Нет",
                year
            ])
    
    return output_file

def main():
    print("=" * 70)
    print("PRISMA PROCESSING - Обработка данных по методологии")
    print("=" * 70)
    
    # Этап 1: Загрузка
    print("\n[1] ЗАГРУЗКА ФАЙЛОВ")
    all_materials = load_all_files()
    print(f"    Загружено: {len(all_materials)} материалов")
    
    # Этап 2: Дедупликация
    print("\n[2] ДЕДУПЛИКАЦИЯ")
    unique, duplicates = deduplicate(all_materials)
    print(f"    Уникальных: {len(unique)}")
    print(f"    Дубликатов удалено: {len(duplicates)}")
    
    # Этап 3: Релевантность
    print("\n[3] ОТБОР ПО РЕЛЕВАНТНОСТИ")
    relevant, irrelevant = filter_relevance(unique)
    print(f"    Релевантных: {len(relevant)}")
    print(f"    Нерелевантных: {len(irrelevant)}")
    
    # Этап 4: Обработка по сферам
    print("\n[4] ОБРАБОТКА ПО СФЕРАМ")
    
    sectors = defaultdict(list)
    for m in relevant:
        sectors[m["_sector"]].append(m)
    
    all_stats = {}
    total_final = 0
    
    for sector, materials in sectors.items():
        processed, stats = process_sector(materials, sector)
        all_stats[sector] = stats
        total_final += len(processed)
        
        # Сохраняем CSV
        csv_file = save_to_csv(processed, sector)
        
        # Сохраняем JSON
        json_file = PROCESSED_DIR / f"{sector}_processed.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump({
                "sector": sector,
                "count": len(processed),
                "materials": processed
            }, f, ensure_ascii=False, indent=2)
        
        print(f"\n    {sector.upper()}:")
        print(f"      Материалов: {len(processed)}")
        print(f"      Качество: высокое={stats['by_quality']['высокий']}, среднее={stats['by_quality']['средний']}, низкое={stats['by_quality']['низкий']}")
        print(f"      CSV: {csv_file.name}")
    
    # Этап 5: Сводная таблица
    print("\n[5] СВОДНАЯ ТАБЛИЦА")
    
    summary_file = TABLES_DIR / "PRISMA_SUMMARY.csv"
    with open(summary_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow([
            "Сфера", "Всего", "Высокое_качество", "Среднее_качество", "Низкое_качество",
            "Яндекс", "Google", "Scholar", "eLibrary", "Академические", "Отраслевые", "СМИ", "Блоги"
        ])
        
        for sector in sorted(sectors.keys()):
            s = all_stats[sector]
            writer.writerow([
                sector.upper(),
                s["total"],
                s["by_quality"]["высокий"],
                s["by_quality"]["средний"],
                s["by_quality"]["низкий"],
                s["by_source"].get("yandex", 0),
                s["by_source"].get("google", 0),
                s["by_source"].get("scholar", 0),
                s["by_source"].get("elibrary", 0),
                s["by_type"].get("academic", 0),
                s["by_type"].get("industry", 0),
                s["by_type"].get("media", 0),
                s["by_type"].get("blog", 0)
            ])
    
    print(f"    Сводная таблица: {summary_file}")
    
    # Итоговый отчёт PRISMA
    print("\n" + "=" * 70)
    print("ИТОГОВЫЙ ОТЧЁТ PRISMA")
    print("=" * 70)
    print(f"""
    ИДЕНТИФИЦИРОВАНО:     {len(all_materials)}
    ПОСЛЕ ДЕДУПЛИКАЦИИ:   {len(unique)} (-{len(duplicates)} дублей)
    ПОСЛЕ РЕЛЕВАНТНОСТИ:  {len(relevant)} (-{len(irrelevant)} нерелевантных)
    
    ИТОГОВАЯ ВЫБОРКА:     {total_final} материалов
    
    РАСПРЕДЕЛЕНИЕ ПО СФЕРАМ:""")
    
    for sector in sorted(sectors.keys()):
        s = all_stats[sector]
        print(f"      {sector.upper()}: {s['total']}")
    
    # Сохраняем отчёт
    report = {
        "methodology": "PRISMA",
        "processed_at": datetime.now().isoformat(),
        "stages": {
            "identified": len(all_materials),
            "after_deduplication": len(unique),
            "after_relevance": len(relevant),
            "final": total_final
        },
        "sectors": {k: v["total"] for k, v in all_stats.items()},
        "quality_distribution": {
            "high": sum(s["by_quality"]["высокий"] for s in all_stats.values()),
            "medium": sum(s["by_quality"]["средний"] for s in all_stats.values()),
            "low": sum(s["by_quality"]["низкий"] for s in all_stats.values())
        }
    }
    
    report_file = PROCESSED_DIR / "PRISMA_REPORT.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    print(f"\n    Полный отчёт: {report_file}")

if __name__ == "__main__":
    main()
