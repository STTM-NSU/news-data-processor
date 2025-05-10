import json
import os
import re
import logging
from collections import defaultdict, Counter
from datetime import datetime, timedelta

import nltk
import psycopg2
import psycopg2.extras
import redis
import yaml
from dotenv import load_dotenv
from natasha import Segmenter, NewsEmbedding, NewsMorphTagger, Doc, MorphVocab
from nltk.corpus import stopwords

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

nltk.download('punkt')
nltk.download('stopwords')

stop_words = set(stopwords.words("russian"))
segmenter = Segmenter()
emb = NewsEmbedding()
morph_tagger = NewsMorphTagger(emb)
morph_vocab = MorphVocab()

load_dotenv()

with open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

base_channel = config.get('redisChannelName', 'scrapper')
partition_count = config.get('partitionsCount', 5)

try:
    conn = psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB_NAME'),
        user=os.getenv('POSTGRES_USERNAME'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
    )
    conn.autocommit = True
    cursor = conn.cursor()
except Exception as e:
    logging.error(f"Ошибка подключения к PostgreSQL: {e}")
    raise

try:
    r = redis.Redis(
        host=os.getenv('REDIS_HOST'),
        port=int(os.getenv('REDIS_PORT')),
        password=os.getenv('REDIS_PASSWORD'),
        decode_responses=True
    )
    p = r.pubsub()
except Exception as e:
    logging.error(f"Ошибка подключения к Redis: {e}")
    raise


def preprocess(text):
    doc = Doc(text.lower())
    doc.segment(segmenter)
    doc.tag_morph(morph_tagger)

    lemmas = []
    for token in doc.tokens:
        token.lemmatize(morph_vocab)
        if token.lemma and \
                token.lemma.isalpha() and \
                token.lemma not in stop_words and \
                len(token.lemma) > 2 and \
                re.fullmatch(r'[а-яё]+', token.lemma):
            lemmas.append(token.lemma)
    return lemmas


def bag_of_words(corpus_tokens):
    return [Counter(tokens) for tokens in corpus_tokens]


def save_bow_to_postgres(url, batch_hour, bow_dicts):
    try:
        cursor.execute("""
            INSERT INTO news_bows (url, date, bow)
            VALUES (%s, %s, %s)
        """, (url, batch_hour, psycopg2.extras.Json(bow_dicts)))
    except Exception as e:
        logging.error(f"[ERROR][POSTGRES][BOW] {e}")


def set_done_to_postgres(date, scraped_count, scraped_time):
    try:
        cursor.execute("""
            INSERT INTO news_done (date, scraped_count, scraped_time)
            VALUES (%s, %s, %s)
        """, (date, scraped_count, scraped_time))
        logging.info(f"[День завершён] {date} - Записано {scraped_count} новостей за {scraped_time}.")
    except Exception as e:
        logging.error(f"[ERROR][POSTGRES][DONE] {e}")


def handle_text(message):
    if message['type'] != 'message':
        return
    try:
        data = json.loads(message['data'])
        url = data.get('url')
        date_str = data.get('date')
        text = data.get('text', '')
        dt = datetime.fromisoformat(date_str)

        tokens = preprocess(text)
        bow = bag_of_words([tokens])
        save_bow_to_postgres(url, dt, bow)

    except Exception as e:
        logging.error(f"[ERROR][TEXT] {e}")


def parse_scraped_time(scraped_time_str):
    # Match patterns for minutes (m) and seconds (s)
    pattern = re.compile(r'(?:(\d+)m)?(?:(\d*\.?\d+)s)?')
    match = pattern.match(scraped_time_str)

    if match:
        minutes = match.group(1)
        seconds = match.group(2)

        minutes = int(minutes) if minutes else 0
        seconds = float(seconds) if seconds else 0.0

        return timedelta(minutes=minutes, seconds=seconds)
    else:
        raise ValueError(f"Invalid time format: {scraped_time_str}")
def handle_done(message):
    if message['type'] != 'message':
        return
    try:
        data = json.loads(message['data'])
        done_str = data.get('date')
        dt = datetime.fromisoformat(done_str)
        scraped_count = data.get('count')
        scraped_time_str = data.get('duration')
        scraped_time = parse_scraped_time(scraped_time_str)
        set_done_to_postgres(dt, scraped_count, scraped_time)

    except Exception as e:
        logging.error(f"[ERROR][DONE] {e}")


channels = {f'{base_channel}:{i}': handle_text for i in range(partition_count)}
channels[f'{base_channel}_day_done'] = handle_done
p.subscribe(**channels)

logging.info("[*] Сервис запущен. Ожидание сообщений из Redis...")
p.run_in_thread(sleep_time=0.1)
