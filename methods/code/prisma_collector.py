#!/usr/bin/env python3
"""
PRISMA Collector v2.0
Автоматизированный сбор данных для систематического обзора по методологии PRISMA

Автор: MediaCode22 Research Group
Дата: Март 2025
Лицензия: MIT
"""

import json
import time
import random
import subprocess
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional

# =============================================================================
# КОНФИГУРАЦИЯ
# =============================================================================

class Config:
    """Конфигурация сборщика"""
    
    # Директории
    OUTPUT_DIR = Path(__file__).parent.parent.parent / "data" / "raw"
    
    # Параметры сбора
    RESULTS_PER_QUERY = 10  # Результатов на запрос (первый экран выдачи)
    PAUSE_MIN = 3.0         # Минимальная пауза между запросами (сек)
    PAUSE_MAX = 6.0         # Максимальная пауза между запросами (сек)
    MAX_RETRIES = 3         # Максимальное количество повторных попыток
    
    # Источники (метки для классификации)
    SOURCES = {
        "yandex": {"lang": "ru", "type": "search"},
        "google": {"lang": "en", "type": "search"},
        "scholar": {"lang": "en", "type": "academic"},
        "elibrary": {"lang": "ru", "type": "academic"}
    }


# =============================================================================
# ПОИСКОВЫЕ ЗАПРОСЫ
# =============================================================================

SEARCH_QUERIES = {
    # =========================================================================
    # СФЕРА 1: ИНФОРМАЦИОННЫЕ ТЕХНОЛОГИИ (IT)
    # =========================================================================
    "IT": {
        "yandex": [  # Русскоязычные запросы
            "принятие решений IT информационные технологии",
            "автоматизация принятия решений IT",
            "цифровая трансформация принятие решений",
            "бизнес-аналитика принятие решений",
            "Big Data принятие решений",
            "машинное обучение принятие решений бизнес",
            "алгоритмы принятия решений IT",
            "информационные системы поддержки решений",
            "agile управление проектами",
            "devops автоматизация",
            "облачные технологии бизнес",
            "микросервисная архитектура решения",
            "системы поддержки принятия решений",
            "интеллектуальный анализ данных бизнес",
            "цифровые технологии управления",
        ],
        "google": [  # Англоязычные запросы
            "decision support systems information technology",
            "IT decision making frameworks",
            "digital transformation decision process",
            "business intelligence decision making",
            "data-driven decision making",
            "artificial intelligence decision support",
            "machine learning business decisions",
            "IT governance decision making",
            "agile project management tools",
            "devops decision automation",
            "cloud migration strategy enterprise",
            "enterprise decision support software",
            "business intelligence tools comparison",
            "data analytics decision making",
        ],
        "scholar": [  # Академические запросы
            "decision support system IT implementation",
            "information systems decision making process",
            "digital decision making frameworks",
            "technology adoption decision models",
            "software architecture decision models",
            "IT governance frameworks",
            "decision support systems survey 2024",
        ],
    },
    
    # =========================================================================
    # СФЕРА 2: ФИНАНСЫ
    # =========================================================================
    "finance": {
        "yandex": [
            "принятие финансовых решений модели",
            "инвестиционный анализ принятие решений",
            "финансовый риск-менеджмент",
            "финансовое планирование инструменты",
            "инвестиционный портфель управление",
            "банковская аналитика системы",
            "финтех инновации банки",
            "криптовалюта инвестиционный анализ",
            "регуляторные технологии финтех",
            "личные финансы приложения",
        ],
        "google": [
            "financial decision making models",
            "investment decision analysis",
            "risk management decision process",
            "financial technology decision support",
            "banking decision systems",
            "portfolio optimization decisions",
            "financial analytics business intelligence",
            "fintech decision making",
            "fintech innovation banking",
            "cryptocurrency investment analysis",
            "regtech compliance automation",
            "personal finance apps",
        ],
        "scholar": [
            "financial decision support systems",
            "behavioral finance decision making",
            "investment decision frameworks",
            "financial risk assessment models",
            "fintech adoption financial services",
            "digital banking transformation",
        ],
    },
    
    # =========================================================================
    # СФЕРА 3: МАРКЕТИНГ
    # =========================================================================
    "marketing": {
        "yandex": [
            "маркетинговые решения аналитика",
            "потребительское поведение анализ",
            "цифровой маркетинг стратегия",
            "маркетинговая автоматизация",
            "аналитика клиентов платформы",
            "исследование рынка методы",
            "программатик реклама платформы",
            "путь клиента аналитика",
            "атрибуция маркетинг модель",
            "аналитика социальных сетей",
        ],
        "google": [
            "marketing decision making analytics",
            "consumer behavior decision process",
            "digital marketing strategy decisions",
            "marketing automation decision tools",
            "customer analytics platforms",
            "market research methodology",
            "programmatic advertising platforms",
            "customer journey analytics",
            "attribution modeling marketing",
            "social media analytics tools",
        ],
        "scholar": [
            "marketing decision support systems",
            "consumer choice modeling",
            "marketing analytics frameworks",
            "advertising effectiveness measurement",
            "programmatic advertising effectiveness",
            "customer journey measurement",
        ],
    },
    
    # =========================================================================
    # СФЕРА 4: МЕДИЦИНА
    # =========================================================================
    "medicine": {
        "yandex": [
            "принятие медицинских решений",
            "клинические решения диагностика",
            "телемедицина цифровые технологии",
            "медицинские информационные системы",
            "диагностика искусственный интеллект",
            "цифровое здравоохранение",
            "персонализированная медицина аналитика",
            "медицинские данные обмен",
            "медицинская визуализация ИИ",
            "мониторинг пациентов системы",
        ],
        "google": [
            "medical decision making",
            "clinical decision support systems",
            "healthcare analytics data",
            "health information systems decision",
            "medical AI diagnosis support",
            "electronic health records analytics",
            "precision medicine analytics",
            "health data interoperability",
            "medical imaging AI",
            "patient monitoring systems",
        ],
        "scholar": [
            "clinical decision support systems effectiveness",
            "medical diagnostic accuracy",
            "healthcare decision frameworks",
            "evidence-based medicine decisions",
            "precision medicine data-driven",
            "health informatics interoperability",
            "healthcare AI applications",
        ],
    },
    
    # =========================================================================
    # СФЕРА 5: ИНЖЕНЕРИЯ
    # =========================================================================
    "engineering": {
        "yandex": [
            "инженерные решения проектирование",
            "технические решения производство",
            "промышленная безопасность решения",
            "программное обеспечение проектирование",
            "САПР системы поддержка решений",
            "производственная аналитика индустрия",
            "цифровой двойник производство",
            "предиктивное обслуживание IoT",
            "SCADA системы аналитика",
            "PLM системы сравнение",
        ],
        "google": [
            "engineering decision making",
            "technical design decisions",
            "industrial decision frameworks",
            "engineering design software tools",
            "CAD decision support systems",
            "manufacturing analytics Industry 4.0",
            "digital twin manufacturing",
            "predictive maintenance IoT",
            "SCADA systems analytics",
            "PLM software comparison",
        ],
        "scholar": [
            "engineering decision support systems",
            "design optimization methods",
            "industrial decision frameworks",
            "manufacturing decision models",
            "digital twin industrial applications",
            "predictive maintenance algorithms",
        ],
    },
    
    # =========================================================================
    # СФЕРА 6: ОБРАЗОВАНИЕ
    # =========================================================================
    "education": {
        "yandex": [
            "образовательные решения педагогика",
            "цифровое образование технологии",
            "педагогические решения методики",
            "образовательные технологии выбор",
            "системы управления обучением",
            "аналитика образования",
            "адаптивное обучение технологии",
            "информационные системы образования",
            "анализ образовательных данных",
            "массовые онлайн курсы аналитика",
        ],
        "google": [
            "educational decision making",
            "digital education technology",
            "learning analytics frameworks",
            "educational technology decision making",
            "learning management systems comparison",
            "academic analytics tools",
            "adaptive learning technology",
            "student information systems",
            "educational data mining",
            "MOOC analytics platforms",
        ],
        "scholar": [
            "educational decision support systems",
            "learning analytics frameworks",
            "educational technology adoption",
            "academic decision models",
            "adaptive learning personalization",
            "educational data mining LA",
            "academic performance analytics",
        ],
    },
    
    # =========================================================================
    # КЕЙС: ТЕХНОЛОГИЧЕСКИЕ ПУЗЫРИ (BUBBLES)
    # =========================================================================
    "bubbles": {
        "yandex": [
            "пузырь Ethernet сетевые технологии",
            "пузыри в экономике финансах",
            "финансовые пузыри detection",
            "рыночные пузыри анализ",
            "спекулятивные пузыри прогноз",
        ],
        "google": [
            "Ethernet bubble network technology",
            "economic bubbles detection",
            "financial bubbles analysis",
            "market bubble prediction",
            "speculative bubble forecasting",
        ],
        "scholar": [
            "network architecture decision models",
            "financial bubble detection methods",
            "speculative bubble analysis",
            "network infrastructure optimization",
        ],
    },
}


# =============================================================================
# КЛАСС СБОРЩИКА
# =============================================================================

class PRISMACollector:
    """
    Автоматизированный сборщик данных по методологии PRISMA
    
    Использует z-ai CLI для выполнения веб-поиска и сохранения результатов.
    Поддерживает несколько источников с настраиваемыми паузами между запросами.
    """
    
    def __init__(self, config: Config = None):
        self.config = config or Config()
        self.output_dir = Path(self.config.OUTPUT_DIR)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.stats = {
            "total_queries": 0,
            "total_results": 0,
            "errors": [],
            "by_sector": {},
            "by_source": {}
        }
    
    def _pause(self) -> None:
        """Случайная пауза между запросами для предотвращения блокировок"""
        delay = random.uniform(self.config.PAUSE_MIN, self.config.PAUSE_MAX)
        print(f"  ⏳ Пауза {delay:.1f} сек...")
        time.sleep(delay)
    
    def _execute_search(self, query: str, output_file: Path) -> int:
        """
        Выполняет веб-поиск через z-ai CLI
        
        Args:
            query: Поисковый запрос
            output_file: Путь для сохранения результатов
            
        Returns:
            Количество найденных результатов
        """
        cmd = [
            "z-ai", "function",
            "-n", "web_search",
            "-a", json.dumps({
                "query": query, 
                "num": self.config.RESULTS_PER_QUERY
            }),
            "-o", str(output_file)
        ]
        
        for attempt in range(self.config.MAX_RETRIES):
            try:
                result = subprocess.run(
                    cmd, 
                    capture_output=True, 
                    text=True, 
                    timeout=60
                )
                
                if result.returncode == 0 and output_file.exists():
                    with open(output_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        count = len(data) if isinstance(data, list) else 1
                        return count
                
                # Повторная попытка при ошибке
                if "429" in result.stderr:  # Rate limit
                    print("  ⚠️ Rate limit, ожидание 10 сек...")
                    time.sleep(10)
                    continue
                    
            except subprocess.TimeoutExpired:
                print(f"  ⚠️ Таймаут, попытка {attempt + 1}/{self.config.MAX_RETRIES}")
            except Exception as e:
                print(f"  ❌ Ошибка: {str(e)[:100]}")
        
        return 0
    
    def collect_sector(self, sector: str, queries: Dict[str, List[str]]) -> Dict:
        """
        Собирает данные для одной отрасли
        
        Args:
            sector: Название отрасли
            queries: Словарь запросов по источникам
            
        Returns:
            Статистика сбора по отрасли
        """
        print(f"\n{'='*60}")
        print(f"📊 СФЕРА: {sector.upper()}")
        print(f"{'='*60}")
        
        sector_stats = {
            "sector": sector,
            "total": 0,
            "by_source": {}
        }
        
        file_counter = 1
        
        for source, query_list in queries.items():
            print(f"\n  🔍 Источник: {source.upper()}")
            source_count = 0
            
            for i, query in enumerate(query_list, 1):
                # Формируем имя файла
                output_file = self.output_dir / f"{sector}_{source}_{file_counter}.json"
                
                print(f"    [{i}/{len(query_list)}] {query[:50]}...")
                
                # Выполняем поиск
                count = self._execute_search(query, output_file)
                source_count += count
                file_counter += 1
                
                # Пауза между запросами
                self._pause()
            
            sector_stats["by_source"][source] = source_count
            sector_stats["total"] += source_count
            print(f"  ✅ {source.upper()}: {source_count} результатов")
        
        self.stats["by_sector"][sector] = sector_stats
        return sector_stats
    
    def collect_all(self, sectors: List[str] = None) -> Dict:
        """
        Выполняет полный сбор данных по всем отраслям
        
        Args:
            sectors: Список отраслей для сбора (None = все)
            
        Returns:
            Полная статистика сбора
        """
        print("=" * 60)
        print("🚀 PRISMA COLLECTOR v2.0")
        print("=" * 60)
        print(f"📁 Директория: {self.output_dir}")
        print(f"⏱️  Паузы: {self.config.PAUSE_MIN}-{self.config.PAUSE_MAX} сек")
        print(f"📊 Результатов на запрос: {self.config.RESULTS_PER_QUERY}")
        print()
        
        sectors_to_collect = sectors or list(SEARCH_QUERIES.keys())
        
        for sector in sectors_to_collect:
            if sector in SEARCH_QUERIES:
                self.collect_sector(sector, SEARCH_QUERIES[sector])
        
        # Итоговая статистика
        self._print_summary()
        self._save_stats()
        
        return self.stats
    
    def _print_summary(self) -> None:
        """Выводит итоговую статистику сбора"""
        total = sum(s["total"] for s in self.stats["by_sector"].values())
        
        print("\n" + "=" * 60)
        print("📊 ИТОГОВАЯ СТАТИСТИКА")
        print("=" * 60)
        
        for sector, stats in self.stats["by_sector"].items():
            print(f"  {sector.upper()}: {stats['total']} материалов")
        
        print(f"\n  ВСЕГО СОБРАНО: {total} материалов")
        print("=" * 60)
    
    def _save_stats(self) -> None:
        """Сохраняет статистику сбора в JSON"""
        stats_file = self.output_dir / f"collection_stats_{self.session_id}.json"
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(self.stats, f, ensure_ascii=False, indent=2)
        print(f"📁 Статистика сохранена: {stats_file}")


# =============================================================================
# ТОЧКА ВХОДА
# =============================================================================

def main():
    """Главная функция запуска сборщика"""
    collector = PRISMACollector()
    collector.collect_all()


if __name__ == "__main__":
    main()
