import psycopg2
import json
import time
import datetime
import multiprocessing


def create_tables():
    start_time = time.time()
    cur_start_time = start_time

    print("===============")
    print("CREATING TABLES")
    print("===============")

    connection = psycopg2.connect(connection_string)
    cursor = connection.cursor()

    cursor.execute("""
    DROP TABLE IF EXISTS context_domains
    """)
    cursor.execute("""
    CREATE TABLE context_domains(
        id INT8 PRIMARY KEY,
        name VARCHAR(255),
        description TEXT
    )
    """)

    cursor.execute("""
    DROP TABLE IF EXISTS context_annotations
    """)
    cursor.execute("""
    CREATE TABLE context_annotations(
        id INT8 PRIMARY KEY,
        conversation_id INT8,
        context_domain_id INT8,
        context_entity_id INT8
    )
    """)

    cursor.execute("""
    DROP TABLE IF EXISTS context_entities
    """)
    cursor.execute("""
    CREATE TABLE context_entities(
        id INT8 PRIMARY KEY,
        name VARCHAR(255),
        description TEXT
    )
    """)

    cursor.execute("""
    DROP TABLE IF EXISTS context_entities
    """)
    cursor.execute("""
    CREATE TABLE context_entities(
        id INT8 PRIMARY KEY,
        name VARCHAR(255),
        description TEXT
    )
    """)

    cursor.execute("""
    DROP TABLE IF EXISTS authors
    """)
    cursor.execute("""
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

    cursor.execute("""
    DROP TABLE IF EXISTS hashtags
    """)
    cursor.execute("""
    CREATE TABLE hashtags(
        id BIGSERIAL PRIMARY KEY,
        tag TEXT
    )
    """)

    cursor.execute("""
    DROP TABLE IF EXISTS conversations_hashtags
    """)
    cursor.execute("""
    CREATE TABLE conversations_hashtags(
        id INT8 PRIMARY KEY,
        conversation_id INT8 UNIQUE,
        hashtag_id INT8
    )
    """)

    cursor.execute("""
    DROP TABLE IF EXISTS annotations
    """)
    cursor.execute("""
    CREATE TABLE annotations(
        id INT8 PRIMARY KEY,
        conversation_id INT8,
        value TEXT,
        type TEXT,
        probability numeric(4, 3)
    )
    """)

    cursor.execute("""
    DROP TABLE IF EXISTS links
    """)
    cursor.execute("""
    CREATE TABLE links(
        id INT8 PRIMARY KEY,
        conversation_id INT8,
        url VARCHAR(2048),
        title TEXT,
        description TEXT
    )
    """)

    cursor.execute("""
    DROP TABLE IF EXISTS conversation_references
    """)
    cursor.execute("""
    CREATE TABLE conversation_references(
        id INT8 PRIMARY KEY,
        conversation_id INT8,
        parent_id INT8,
        type VARCHAR(20)
    )
    """)

    cursor.execute("""
    DROP TABLE IF EXISTS conversations
    """)
    cursor.execute("""
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

    connection.commit()

    cursor.close()
    connection.close()

    header = "Main process created emtpy tables in twitter database"
    print_execution_time(header, start_time, cur_start_time)


def print_execution_time(header, start_time, cur_start_time):
    end_time = time.time()
    cur_time = end_time - cur_start_time
    total_time = end_time - start_time
    cur_date = datetime.datetime.now()

    total_minutes = int(total_time // 60)
    total_seconds = round(total_time) - total_minutes * 60
    cur_minutes = int(cur_time // 60)
    cur_seconds = round(cur_time) - cur_minutes * 60
    print("[{}]:{}T{}Z;{:02d}:{:02d};{:02d}:{:02d}"
          .format(header, cur_date.strftime("%Y-%m-%d"), cur_date.strftime("%H:%M"),
                  total_minutes, total_seconds, cur_minutes, cur_seconds))

    return total_time


def proc_insert_authors(args):
    author_lines, start_time, connection_string = args

    connection = psycopg2.connect(connection_string)
    cursor = connection.cursor()

    cur_start_time = time.time()

    authors_id_arr = []
    authors_insert_count = 0
    authors_insert_arr = []
    authors_sql_query = """
    INSERT INTO 
        authors (id, name, username, description, followers_count, following_count, tweet_count, listed_count)
    VALUES
    """

    hashtags_tag_arr = []
    hashtags_insert_count = 0
    hashtags_insert_arr = []
    hashtags_sql_query = """
    INSERT INTO
        hashtags (tag)
    VALUES 
    """

    for author_line in author_lines:
        author = json.loads(author_line)
        author_public_metrics = author["public_metrics"]

        author_id = author["id"]
        if author_id in authors_id_arr:
            continue
        authors_id_arr.append(author_id)

        name = author["name"].replace("\x00", "\uFFFD")
        username = author["username"].replace("\x00", "\uFFFD")
        description = author["username"].replace("\x00", "\uFFFD")
        followers_count = author_public_metrics["followers_count"]
        following_count = author_public_metrics["following_count"]
        tweet_count = author_public_metrics["tweet_count"]
        listed_count = author_public_metrics["listed_count"]

        authors_insert_arr.extend([author_id, name, username, description,
                                   followers_count, following_count, tweet_count, listed_count])

        if authors_insert_count > 0:
            authors_sql_query += ", (%s, %s,%s, %s, %s, %s, %s, %s)"
        else:
            authors_sql_query += "(%s, %s,%s, %s, %s, %s, %s, %s)"

        author_hashtags = None
        try:
            author_hashtags = author["entities"]["description"]["hashtags"]
        except KeyError:
            pass

        if author_hashtags is not None:
            for author_hashtag in author_hashtags:
                tag = author_hashtag["tag"]
                if tag in hashtags_tag_arr:
                    continue
                hashtags_tag_arr.append(tag)

                hashtags_insert_arr.extend([tag])

                if hashtags_insert_count > 0:
                    hashtags_sql_query += ", (%s)"
                else:
                    hashtags_sql_query += "(%s)"

                hashtags_insert_count += 1

        authors_insert_count += 1

    if hashtags_insert_count > 0:
        hashtags_sql_query += "ON CONFLICT DO NOTHING"
        hashtags_tuple = tuple(hashtags_insert_arr)
        cursor.execute(hashtags_sql_query, hashtags_tuple)

    if authors_insert_count > 0:

        authors_sql_query += "ON CONFLICT DO NOTHING"
        author_tuple = tuple(authors_insert_arr)
        cursor.execute(authors_sql_query, author_tuple)

        connection.commit()
        header = "Process {} processed {} author lines".format(multiprocessing.current_process().pid,
                                                               authors_insert_count)
        print_execution_time(header, start_time, cur_start_time)

    cursor.close()
    connection.close()


def insert_authors():
    print("=================")
    print("INSERTING AUTHORS")
    print("=================")

    multiprocessing.current_process()

    start_time = time.time()
    cur_start_time = start_time

    author_lines_arr = []
    author_lines = []
    author_lines_count = 0
    for author_line in open('authors.jsonl', 'r'):
        author_lines.append(author_line)
        author_lines_count += 1

        if author_lines_count == 10000:
            author_lines_arr.append(author_lines)
            author_lines_count = 0
            author_lines = []

    args = [(author_lines, start_time, connection_string) for author_lines in author_lines_arr]

    header = "Main process read author.jsonl file"
    print_execution_time(header, start_time, cur_start_time)

    # for author_lines in author_lines_arr:
    #     proc_insert_authors([author_lines, start_time, connection_string])

    with multiprocessing.Pool() as pool:
        pool.map(proc_insert_authors, args)


def insert_conversations():
    print("=======================")
    print("INSERTING CONVERSATIONS")
    print("=======================")

    connection = psycopg2.connect(connection_string)
    cursor = connection.cursor()

    conversations_id_arr = []
    for conversation_line in open('authors.jsonl', 'r'):
        conversation = json.loads(conversation_line)

        conversation_id = conversation["id"]
        if conversation_id in conversations_id_arr:
            continue
        conversations_id_arr.append(conversation_id)

    cursor.close()
    connection.close()


def proc_test(args):
    author_lines, start_time, connection_string = args

    connection = psycopg2.connect(connection_string)
    cursor = connection.cursor()

    cursor.close()
    connection.close()


if __name__ == '__main__':
    connection_string = "dbname=twitter user=postgres password=postgres"

    # author_lines = [[1, 2, 3], [21, 22, 23], [31, 32, 33]]
    # start_time = time.time()
    # args = [(author_line, start_time, connection_string) for author_line in author_lines]
    #
    # with multiprocessing.Pool() as pool:
    #     pool.map(proc_test, args)

    create_tables()
    insert_authors()
    # insert_conversations(connection)
