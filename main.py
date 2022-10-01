import psycopg2
import json
import time
import datetime


def create_tables(conn):
    cur = conn.cursor()

    cur.execute("""
    DROP TABLE IF EXISTS context_domains
    """)
    cur.execute("""
    CREATE TABLE context_domains(
        id INT8 PRIMARY KEY,
        name VARCHAR(255),
        description TEXT
    )
    """)
    conn.commit()

    cur.execute("""
    DROP TABLE IF EXISTS context_annotations
    """)
    cur.execute("""
    CREATE TABLE context_annotations(
        id INT8 PRIMARY KEY,
        conversation_id INT8,
        context_domain_id INT8,
        context_entity_id INT8
    )
    """)
    conn.commit()

    cur.execute("""
    DROP TABLE IF EXISTS context_entities
    """)
    cur.execute("""
    CREATE TABLE context_entities(
        id INT8 PRIMARY KEY,
        name VARCHAR(255),
        description TEXT
    )
    """)
    conn.commit()

    cur.execute("""
    DROP TABLE IF EXISTS context_entities
    """)
    cur.execute("""
    CREATE TABLE context_entities(
        id INT8 PRIMARY KEY,
        name VARCHAR(255),
        description TEXT
    )
    """)
    conn.commit()

    cur.execute("""
    DROP TABLE IF EXISTS authors
    """)
    cur.execute("""
    CREATE TABLE authors(
        id INT8 PRIMARY KEY,
        name VARCHAR(255),
        username VARCHAR(255),
        description TEXT,
        followers_count INT4,
        following_count INT4,
        tweet_count INT4,
        listed_count INT4
    )
    """)
    conn.commit()

    cur.execute("""
    DROP TABLE IF EXISTS hashtags
    """)
    cur.execute("""
    CREATE TABLE hashtags(
        id BIGSERIAL PRIMARY KEY,
        tag TEXT
    )
    """)
    conn.commit()

    cur.execute("""
    DROP TABLE IF EXISTS conversations_hashtags
    """)
    cur.execute("""
    CREATE TABLE conversations_hashtags(
        id INT8 PRIMARY KEY,
        conversation_id INT8 UNIQUE,
        hashtag_id INT8
    )
    """)
    conn.commit()

    cur.execute("""
    DROP TABLE IF EXISTS annotations
    """)
    cur.execute("""
    CREATE TABLE annotations(
        id INT8 PRIMARY KEY,
        conversation_id INT8,
        value TEXT,
        type TEXT,
        probability numeric(4, 3)
    )
    """)
    conn.commit()

    cur.execute("""
    DROP TABLE IF EXISTS links
    """)
    cur.execute("""
    CREATE TABLE links(
        id INT8 PRIMARY KEY,
        conversation_id INT8,
        url VARCHAR(2048),
        title TEXT,
        description TEXT
    )
    """)
    conn.commit()

    cur.execute("""
    DROP TABLE IF EXISTS conversation_references
    """)
    cur.execute("""
    CREATE TABLE conversation_references(
        id INT8 PRIMARY KEY,
        conversation_id INT8,
        parent_id INT8,
        type VARCHAR(20)
    )
    """)
    conn.commit()

    cur.execute("""
    DROP TABLE IF EXISTS conversations
    """)
    cur.execute("""
    CREATE TABLE conversations(
        id INT8 PRIMARY KEY,
        author_id INT8,
        content TEXT,
        possibly_sensitive BOOL,
        language VARCHAR(3),
        source TEXT,
        retweet_count INT4,
        reply_count INT4,
        like_count INT4,
        quote_count INT4,
        created_at TIMESTAMP WITH TIME ZONE
    )
    """)
    conn.commit()

    cur.close()


def insert_authors(conn):
    cur = conn.cursor()

    start_time = time.time()
    total_time = 0

    inserted_count = 0
    id_arr = []
    for line in open('authors.jsonl', 'r'):
        author = json.loads(line)
        author_public_metrics = author["public_metrics"]

        id = author["id"]
        if id in id_arr:
            continue
        id_arr.append(id)

        name = author["name"].replace("\x00", "\uFFFD")
        username = author["username"].replace("\x00", "\uFFFD")
        description = author["username"].replace("\x00", "\uFFFD")
        followers_count = author_public_metrics["followers_count"]
        following_count = author_public_metrics["following_count"]
        tweet_count = author_public_metrics["tweet_count"]
        listed_count = author_public_metrics["listed_count"]

        cur.execute(
            """
            INSERT INTO authors (
                id,
                name,
                username,
                description,
                followers_count,
                following_count,
                tweet_count,
                listed_count    
            ) VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
            """,
            (id, name, username, description, followers_count, following_count, tweet_count, listed_count)
        )
        conn.commit()

        author_hashtags = None
        try:
            author_hashtags = author["entities"]["description"]["hashtags"]
        except KeyError:
            pass

        if author_hashtags is not None:
            for author_hashtag in author_hashtags:
                tag = author_hashtag["tag"]

                cur.execute(
                    """
                    INSERT INTO hashtags (
                        tag  
                    ) VALUES (
                        %s
                    ) ON CONFLICT DO NOTHING
                    """, (tag,)
                )

            conn.commit()

        inserted_count += 1
        if inserted_count == 10000:
            end_time = time.time()
            cur_time = end_time - start_time
            total_time += cur_time
            cur_date = datetime.datetime.now()

            total_minutes = round(total_time / 60)
            total_seconds = round(total_time - total_time/60)
            cur_minutes = round(cur_time / 60)
            cur_seconds = round(cur_time - cur_time/60)
            print("{}T{}Z;{:02d}:{:02d};{:02d}:{:02d}"
                  .format(cur_date.strftime("%Y-%m-%d"), cur_date.strftime("%H:%M"),
                          total_minutes, total_seconds, cur_minutes, cur_seconds))

            start_time = time.time()
            inserted_count = 0

    cur.close()


def insert_conversations(conn):
    cur = conn.cursor()

    id_arr = []
    for line in open('authors.jsonl', 'r'):
        conversation = json.loads(line)

        id = conversation["id"]
        if id in id_arr:
            continue
        id_arr.append(id)

    cur.close()


if __name__ == '__main__':
    connection = psycopg2.connect("dbname=twitter user=postgres password=postgres")

    create_tables(connection)
    insert_authors(connection)
    # insert_conversations(connection)

    connection.close()
