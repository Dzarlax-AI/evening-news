-- Evening News v2 Database Schema

-- Источники новостей
CREATE TABLE sources (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    source_type VARCHAR(50) NOT NULL, -- rss, telegram, reddit, twitter, api
    url TEXT NOT NULL,
    enabled BOOLEAN DEFAULT true,
    config JSONB DEFAULT '{}', -- Специфичные настройки для каждого типа
    fetch_interval INTEGER DEFAULT 1800, -- Интервал обновления в секундах
    last_fetch TIMESTAMP,
    last_success TIMESTAMP,
    last_error TEXT,
    error_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Сырые статьи из источников
CREATE TABLE articles (
    id SERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES sources(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    content TEXT,
    summary TEXT, -- Краткое описание из источника
    category VARCHAR(50), -- Business, Tech, Science, Nature, Serbia, Marketing, Other
    image_url TEXT,
    published_at TIMESTAMP,
    fetched_at TIMESTAMP DEFAULT NOW(),
    processed BOOLEAN DEFAULT false,
    summary_processed BOOLEAN DEFAULT FALSE,
    category_processed BOOLEAN DEFAULT FALSE,
    hash_content VARCHAR(64), -- Хеш для дедупликации
    
    -- Advertising detection fields
    is_advertisement BOOLEAN DEFAULT FALSE,
    ad_confidence REAL DEFAULT 0.0, -- Confidence score (0.0-1.0)
    ad_type VARCHAR(50), -- Type of advertising (product_promotion, affiliate_marketing, etc.)
    ad_reasoning TEXT, -- AI reasoning for advertising classification
    ad_markers JSONB DEFAULT '[]', -- List of advertising markers found
    ad_processed BOOLEAN DEFAULT FALSE, -- True if advertising detection was attempted
    
    UNIQUE(url)
);

-- Кластеры убраны - функциональность не используется

-- Дневные сводки по категориям
CREATE TABLE daily_summaries (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    category VARCHAR(50) NOT NULL, -- Business, Tech, Science, Nature, Serbia, Marketing, Other
    summary_text TEXT NOT NULL,
    articles_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(date, category)
);

-- Настройки системы
CREATE TABLE settings (
    key VARCHAR(255) PRIMARY KEY,
    value JSONB NOT NULL,
    description TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Статистика работы
CREATE TABLE processing_stats (
    id SERIAL PRIMARY KEY,
    date DATE DEFAULT CURRENT_DATE,
    articles_fetched INTEGER DEFAULT 0,
    articles_processed INTEGER DEFAULT 0,
    clusters_created INTEGER DEFAULT 0,
    clusters_updated INTEGER DEFAULT 0,
    api_calls_made INTEGER DEFAULT 0,
    errors_count INTEGER DEFAULT 0,
    processing_time_seconds INTEGER DEFAULT 0,
    UNIQUE(date)
);

-- Настройки расписания автоматических задач
CREATE TABLE schedule_settings (
    id SERIAL PRIMARY KEY,
    task_name VARCHAR(100) NOT NULL UNIQUE,
    enabled BOOLEAN DEFAULT FALSE,
    
    -- Schedule configuration
    schedule_type VARCHAR(20) DEFAULT 'daily',
    hour INTEGER DEFAULT 9 CHECK (hour >= 0 AND hour <= 23),
    minute INTEGER DEFAULT 0 CHECK (minute >= 0 AND minute <= 59),
    weekdays JSONB DEFAULT '[]',
    timezone VARCHAR(50) DEFAULT 'Europe/Belgrade',
    
    -- Task specific settings
    task_config JSONB DEFAULT '{}',
    
    last_run TIMESTAMP,
    last_finished_at TIMESTAMP,
    last_success_at TIMESTAMP,
    last_status VARCHAR(20),
    last_error TEXT,
    last_duration_seconds NUMERIC(12, 3),
    next_run TIMESTAMP,
    is_running BOOLEAN DEFAULT FALSE,
    
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Индексы для производительности
CREATE INDEX idx_articles_url ON articles(url);
CREATE INDEX idx_articles_published_at ON articles(published_at DESC);
CREATE INDEX idx_articles_processed ON articles(processed);
CREATE INDEX idx_articles_source_id ON articles(source_id);
CREATE INDEX idx_articles_category ON articles(category);
CREATE INDEX idx_daily_summaries_date ON daily_summaries(date DESC);
CREATE INDEX idx_daily_summaries_category ON daily_summaries(category);
CREATE INDEX idx_sources_enabled ON sources(enabled);
CREATE INDEX idx_sources_last_fetch ON sources(last_fetch);
CREATE INDEX idx_schedule_settings_task_name ON schedule_settings(task_name);
CREATE INDEX idx_schedule_settings_enabled ON schedule_settings(enabled);
CREATE INDEX idx_schedule_settings_next_run ON schedule_settings(next_run);
-- Indexes from migrations
CREATE INDEX idx_articles_summary_processed ON articles(summary_processed);
CREATE INDEX idx_articles_category_processed ON articles(category_processed);
CREATE INDEX idx_articles_hash_content ON articles(hash_content);

-- Вставляем настройки по умолчанию
INSERT INTO settings (key, value, description) VALUES
('clustering', '{"similarity_threshold": 0.8, "merge_time_window": 86400, "enable_topic_detection": true}', 'Настройки кластеризации'),
('processing', '{"batch_size": 50, "max_concurrent": 5, "api_rate_limit": 3}', 'Настройки обработки'),
('sources_config', '{"default_fetch_interval": 1800, "max_errors_before_disable": 10}', 'Настройки источников');

-- Вставляем задачи расписания по умолчанию (финальная версия)
INSERT INTO schedule_settings (task_name, enabled, schedule_type, hour, minute, weekdays, timezone, task_config) VALUES
('news_digest', false, 'daily', 9, 0, '[1,2,3,4,5,6,7]', 'Europe/Belgrade', 
 '{
   "sync_sources": true,
   "categorize_articles": true,
   "generate_summaries": true,
   "send_telegram": true,
   "create_telegraph": true,
   "max_articles": 20
 }'),
('news_processing', false, 'hourly', 0, 0, '[1,2,3,4,5,6,7]', 'Europe/Belgrade', 
 '{
   "sync_sources": true,
   "categorize_articles": true,
   "generate_summaries": false,
   "send_telegram": false
 }');

-- Триггер для обновления updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_sources_updated_at BEFORE UPDATE ON sources
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Removed trigger for non-existent news_clusters table

CREATE TRIGGER update_daily_summaries_updated_at BEFORE UPDATE ON daily_summaries
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_schedule_settings_updated_at BEFORE UPDATE ON schedule_settings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Indexes for advertising detection queries
CREATE INDEX idx_articles_is_advertisement ON articles(is_advertisement);
CREATE INDEX idx_articles_ad_type ON articles(ad_type);
CREATE INDEX idx_articles_ad_processed ON articles(ad_processed);
CREATE INDEX idx_articles_source_advertisement ON articles(source_id, is_advertisement);
