import psycopg2
import json
import time
import datetime
import csv
import bisect


LINES_PER_PROC = 10000
MAX_ARRAY_SIZE = 500000000

authors_id_arr = {}
hashtags_tag_arr = {}
hashtags_id_arr = {}
conversations_id_arr = {}
context_domains_id_arr = {}
context_entities_id_arr = {}


def print_execution_time(start_time, cur_start_time, file_output):
    end_time = time.time()
    cur_time = end_time - cur_start_time
    total_time = end_time - start_time
    cur_date = datetime.datetime.now()

    total_minutes = int(total_time // 60)
    total_seconds = int(total_time) - total_minutes * 60
    cur_minutes = int(cur_time // 60)
    cur_seconds = int(cur_time) - cur_minutes * 60

    output_string = "{}T{}Z;{:02d}:{:02d};{:02d}:{:02d}".format(
        cur_date.strftime("%Y-%m-%d"), cur_date.strftime("%H:%M"),
        total_minutes, total_seconds, cur_minutes, cur_seconds)

    print(output_string)

    if file_output is not None:
        writer = csv.writer(file_output)
        new_row = [output_string]
        writer.writerow(new_row)


def create_tables():
    print("===============")
    print("CREATING TABLES")
    print("===============")

    start_time = time.time()
    cur_start_time = start_time

    cursor.execute("""
    DROP TABLE IF EXISTS context_annotations
    """)
    cursor.execute("""
    CREATE TABLE context_annotations(
        id BIGSERIAL PRIMARY KEY,
        conversation_id INT8 NOT NULL,
        context_domain_id INT8 NOT NULL,
        context_entity_id INT8 NOT NULL
    )
    """)

    cursor.execute("""
    DROP TABLE IF EXISTS context_domains
    """)
    cursor.execute("""
    CREATE TABLE context_domains(
        id INT8 PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        description TEXT
    )
    """)

    cursor.execute("""
    DROP TABLE IF EXISTS context_entities
    """)
    cursor.execute("""
    CREATE TABLE context_entities(
        id INT8 PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        description TEXT
    )
    """)

    cursor.execute("""
    DROP TABLE IF EXISTS hashtags
    """)
    cursor.execute("""
    CREATE TABLE hashtags(
        id BIGSERIAL PRIMARY KEY,
        tag TEXT UNIQUE
    )
    """)

    cursor.execute("""
    DROP TABLE IF EXISTS conversation_hashtags
    """)
    cursor.execute("""
    CREATE TABLE conversation_hashtags(
        id BIGSERIAl PRIMARY KEY,
        conversation_id INT8,
        hashtag_id INT8
    )
    """)

    cursor.execute("""
    DROP TABLE IF EXISTS annotations
    """)
    cursor.execute("""
    CREATE TABLE annotations(
        id BIGSERIAL PRIMARY KEY,
        conversation_id INT8 NOT NULL,
        value TEXT NOT NULL,
        type TEXT NOT NULL,
        probability numeric(4, 3) NOT NULL
    )
    """)

    cursor.execute("""
    DROP TABLE IF EXISTS links
    """)
    cursor.execute("""
    CREATE TABLE links(
        id BIGSERIAL PRIMARY KEY,
        conversation_id INT8 NOT NULL,
        url VARCHAR(2048) NOT NULL,
        title TEXT,
        description TEXT
    )
    """)

    cursor.execute("""
    DROP TABLE IF EXISTS conversation_references
    """)
    cursor.execute("""
    CREATE TABLE conversation_references(
        id BIGSERIAl PRIMARY KEY,
        conversation_id INT8 NOT NULL,
        parent_id INT8 NOT NULL,
        type VARCHAR(20) NOT NULL
    )
    """)

    cursor.execute("""
    DROP TABLE IF EXISTS conversations
    """)
    cursor.execute("""
    CREATE TABLE conversations(
        id INT8 PRIMARY KEY,
        author_id INT8 NOT NULL,
        content TEXT NOT NULL,
        possibly_sensitive BOOL NOT NULL,
        language VARCHAR(3) NOT NULL,
        source TEXT NOT NULL,
        retweet_count INT4,
        reply_count INT4,
        like_count INT4,
        quote_count INT4,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL
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

    connection.commit()

    print_execution_time(start_time, cur_start_time, None)


def alter_tables():
    print("===============")
    print("ALTERING TABLES")
    print("===============")

    start_time = time.time()
    cur_start_time = start_time

    connection.commit()

    print_execution_time(start_time, cur_start_time, None)


def proc_insert_authors(args):
    author_lines, authors_output, start_time = args

    cur_start_time = time.time()

    authors_insert_count = 0
    authors_insert_arr = []
    authors_sql_query = """
    INSERT INTO 
        authors (id, name, username, description, followers_count, following_count, tweet_count, listed_count)
    VALUES
    """

    hashtags_insert_count = 0
    hashtags_insert_arr = []
    hashtags_sql_query = """
    INSERT INTO
        hashtags (tag)
    VALUES 
    """

    author_lines_count = 0
    for author_line in author_lines:
        author_lines_count += 1
        author = json.loads(author_line)

        author_id = author["id"]
        if author_id in authors_id_arr:
            continue
        authors_id_arr[author_id] = True

        author_public_metrics = author["public_metrics"]
        name = author["name"].replace("\x00", "\uFFFD")[:255]
        username = author["username"].replace("\x00", "\uFFFD")[:255]
        description = author["username"].replace("\x00", "\uFFFD")
        followers_count = author_public_metrics["followers_count"]
        following_count = author_public_metrics["following_count"]
        tweet_count = author_public_metrics["tweet_count"]
        listed_count = author_public_metrics["listed_count"]

        authors_insert_arr.extend([author_id, name, username, description,
                                   followers_count, following_count, tweet_count, listed_count])

        if authors_insert_count > 0:
            authors_sql_query += ", (%s, %s, %s, %s, %s, %s, %s, %s)"
        else:
            authors_sql_query += "(%s, %s, %s, %s, %s, %s, %s, %s)"

        hashtags = None
        try:
            hashtags = author["entities"]["description"]["hashtags"]
        except KeyError:
            pass

        if hashtags is not None:
            for hashtag in hashtags:
                tag = hashtag["tag"]

                if tag in hashtags_tag_arr:
                    continue
                hashtags_tag_arr[tag] = True

                hashtags_insert_arr.append(tag)
                if hashtags_insert_count > 0:
                    hashtags_sql_query += ", (%s)"
                else:
                    hashtags_sql_query += "(%s)"
                hashtags_insert_count += 1

        authors_insert_count += 1

    if hashtags_insert_count > 0:
        hashtags_sql_query += " RETURNING id"
        hashtags_tuple = tuple(hashtags_insert_arr)
        cursor.execute(hashtags_sql_query, hashtags_tuple)
        hashtag_ids = cursor.fetchall()

        hashtag_insert_arr_key = 0
        for hashtag_id in hashtag_ids:
            hashtag_tag = hashtags_insert_arr[hashtag_insert_arr_key]
            hashtags_id_arr[hashtag_tag] = hashtag_id
            hashtag_insert_arr_key += 1

    if authors_insert_count > 0:
        authors_tuple = tuple(authors_insert_arr)
        cursor.execute(authors_sql_query, authors_tuple)
        connection.commit()

    print_execution_time(start_time, cur_start_time, authors_output)


def insert_authors():
    print("=================")
    print("INSERTING AUTHORS")
    print("=================")

    authors_output = open('authors.csv', 'w')

    writer = csv.writer(authors_output)
    authors_output_header = ["datum; celkovy cas; aktualny_cas"]
    writer.writerow(authors_output_header)

    start_time = time.time()
    author_lines = []
    author_lines_count = 0
    for author_line in open('authors.jsonl', 'r'):
        author_lines.append(author_line)
        author_lines_count += 1

        if author_lines_count == LINES_PER_PROC:
            proc_insert_authors([author_lines, authors_output, start_time])

            author_lines_count = 0
            author_lines = []

    if author_lines_count > 0:
        proc_insert_authors([author_lines, authors_output, start_time])

    authors_output.close()


def proc_insert_conversations(args):
    conversation_lines, conversations_output, start_time = args

    cur_start_time = time.time()

    conversations_insert_count = 0
    conversations_insert_arr = []
    conversations_sql_query = """
    INSERT INTO
        conversations (id, author_id, content, possibly_sensitive, language, source,
        retweet_count, reply_count, like_count, quote_count, created_at)
    VALUES
    """

    authors_insert_count = 0
    authors_insert_arr = []
    authors_sql_query = """
    INSERT INTO 
        authors (id, name, username, description, followers_count, following_count, tweet_count, listed_count)
    VALUES
    """

    hashtags_key_arr = []
    hashtags_insert_count = 0
    hashtags_insert_arr = []
    hashtags_sql_query = """
    INSERT INTO
        hashtags (tag)
    VALUES 
    """

    conversation_hashtags_insert_count = 0
    conversation_hashtags_insert_arr = []
    conversation_hashtags_sql_query = """
    INSERT INTO
        conversation_hashtags (conversation_id, hashtag_id)
    VALUES
    """

    annotations_insert_count = 0
    annotations_insert_arr = []
    annotations_sql_query = """
    INSERT INTO
        annotations (conversation_id, value, type, probability)
    VALUES
    """

    links_insert_count = 0
    links_insert_arr = []
    links_sql_query = """
    INSERT INTO
        links (conversation_id, url, title, description)
    VALUES
    """

    conversation_references_insert_count = 0
    conversation_references_insert_arr = []
    conversation_references_sql_query = """
    INSERT INTO
        conversation_references (conversation_id, parent_id, type)
    VALUES
    """

    context_annotations_insert_count = 0
    context_annotations_insert_arr = []
    context_annotations_sql_query = """
    INSERT INTO
        context_annotations(conversation_id, context_domain_id, context_entity_id)
    VALUES
    """

    context_domains_insert_count = 0
    context_domains_insert_arr = []
    context_domains_sql_query = """
    INSERT INTO
        context_domains (id, name, description)
    VALUES
    """

    context_entities_insert_count = 0
    context_entities_insert_arr = []
    context_entities_sql_query = """
    INSERT INTO
        context_entities (id, name, description)
    VALUES
    """

    conversation_lines_count = 0
    for conversation_line in conversation_lines:
        conversation_lines_count += 1
        conversation = json.loads(conversation_line)

        conversation_id = conversation["id"]
        if conversation_id in conversations_id_arr:
            continue
        conversations_id_arr[conversation_id] = True

        author_not_found = True
        author_id = conversation["author_id"]
        if author_id in authors_id_arr:
            author_not_found = False

        if author_not_found:
            authors_id_arr[author_id] = True

            name = None
            username = None
            description = None
            followers_count = None
            following_count = None
            tweet_count = None
            listed_count = None

            authors_insert_arr.extend([author_id, name, username, description,
                                       followers_count, following_count, tweet_count, listed_count])

            if authors_insert_count > 0:
                authors_sql_query += ", (%s, %s, %s, %s, %s, %s, %s, %s)"
            else:
                authors_sql_query += "(%s, %s, %s, %s, %s, %s, %s, %s)"

            authors_insert_count += 1

        public_metrics = conversation["public_metrics"]
        content = conversation["text"]
        possibly_sensitive = conversation["possibly_sensitive"]
        language = conversation["lang"]
        source = conversation["source"]
        retweet_count = public_metrics["retweet_count"]
        reply_count = public_metrics["reply_count"]
        like_count = public_metrics["like_count"]
        quote_count = public_metrics["quote_count"]
        created_at = conversation["created_at"]

        conversations_insert_arr.extend(
            [conversation_id, author_id, content, possibly_sensitive, language, source,
             retweet_count, reply_count, like_count, quote_count, created_at])

        entities = None
        try:
            entities = conversation["entities"]
        except KeyError:
            pass

        if entities is not None:
            hashtags = None
            try:
                hashtags = entities["hashtags"]
            except KeyError:
                pass

            if hashtags is not None:
                for hashtag in hashtags:
                    tag = hashtag["tag"]

                    conversation_hashtags_insert_arr.extend([conversation_id, tag])
                    if conversation_hashtags_insert_count > 0:
                        conversation_hashtags_sql_query += ", (%s, %s)"
                    else:
                        conversation_hashtags_sql_query += "(%s, %s)"
                    conversation_hashtags_insert_count += 1

                    if tag in hashtags_tag_arr:
                        continue
                    hashtags_tag_arr[tag] = True

                    hashtags_insert_arr.extend([tag])
                    if hashtags_insert_count > 0:
                        hashtags_sql_query += ", (%s)"
                    else:
                        hashtags_sql_query += "(%s)"
                    hashtags_insert_count += 1

            annotations = None
            try:
                annotations = entities["annotations"]
            except KeyError:
                pass

            if annotations is not None:
                for annotation in annotations:
                    value = annotation["normalized_text"]
                    type = annotation["type"]
                    probability = annotation["probability"]

                    annotations_insert_arr.extend([conversation_id, value, type, probability])
                    if annotations_insert_count > 0:
                        annotations_sql_query += ", (%s, %s, %s, %s)"
                    else:
                        annotations_sql_query += "(%s, %s, %s, %s)"

                    annotations_insert_count += 1

            links = None
            try:
                links = entities["urls"]
            except KeyError:
                pass

            if links is not None:
                for link in links:
                    url = link["expanded_url"][:2048]
                    title = None
                    try:
                        title = link["title"]
                    except KeyError:
                        pass
                    description = None
                    try:
                        description = link["description"]
                    except KeyError:
                        pass

                    links_insert_arr.extend([conversation_id, url, title, description])
                    if links_insert_count > 0:
                        links_sql_query += ", (%s, %s, %s, %s)"
                    else:
                        links_sql_query += "(%s, %s, %s, %s)"

                    links_insert_count += 1

            referenced_tweets = None
            try:
                referenced_tweets = conversation["referenced_tweets"]
            except KeyError:
                pass

            if referenced_tweets is not None:
                for referenced_tweet in referenced_tweets:
                    parent_id = referenced_tweet["id"]
                    type = referenced_tweet["type"][:20]

                    conversation_references_insert_arr.extend([conversation_id, parent_id, type])
                    if conversation_references_insert_count > 0:
                        conversation_references_sql_query += ", (%s, %s, %s)"
                    else:
                        conversation_references_sql_query += "(%s, %s, %s)"

                    conversation_references_insert_count += 1

            context_annotations = None
            try:
                context_annotations = conversation["context_annotations"]
            except KeyError:
                pass

            if context_annotations is not None:
                for context_annotation in context_annotations:
                    domain = context_annotation["domain"]

                    domain_id = domain["id"]

                    context_domain_not_inserted = True
                    if domain_id in context_domains_id_arr:
                        context_domain_not_inserted = False

                    if context_domain_not_inserted:
                        context_domains_id_arr[domain_id] = True

                        domain_name = domain["name"]
                        domain_description = None
                        try:
                            domain_description = domain["description"]
                        except KeyError:
                            pass

                        context_domains_insert_arr.extend([domain_id, domain_name, domain_description])
                        if context_domains_insert_count > 0:
                            context_domains_sql_query += ", (%s, %s, %s)"
                        else:
                            context_domains_sql_query += "(%s, %s, %s)"

                        context_domains_insert_count += 1

                    entity = context_annotation["entity"]

                    entity_id = entity["id"]

                    context_entity_not_inserted = True
                    if entity_id in context_entities_id_arr:
                        context_entity_not_inserted = False

                    if context_entity_not_inserted:
                        context_entities_id_arr[entity_id] = True

                        entity_name = entity["name"]
                        entity_description = None
                        try:
                            entity_description = entity["description"]
                        except KeyError:
                            pass

                        context_entities_insert_arr.extend([entity_id, entity_name, entity_description])
                        if context_entities_insert_count > 0:
                            context_entities_sql_query += ", (%s, %s, %s)"
                        else:
                            context_entities_sql_query += "(%s, %s, %s)"

                        context_entities_insert_count += 1

                    context_annotations_insert_arr.extend([conversation_id, domain_id, entity_id])
                    if context_annotations_insert_count > 0:
                        context_annotations_sql_query += ", (%s, %s, %s)"
                    else:
                        context_annotations_sql_query += "(%s, %s, %s)"
                    context_annotations_insert_count += 1

        if conversations_insert_count > 0:
            conversations_sql_query += ", (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        else:
            conversations_sql_query += "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        conversations_insert_count += 1

    if hashtags_insert_count > 0:
        hashtags_sql_query += " RETURNING id"
        hashtags_tuple = tuple(hashtags_insert_arr)
        cursor.execute(hashtags_sql_query, hashtags_tuple)
        hashtag_ids = cursor.fetchall()

        hashtag_insert_arr_key = 0
        for hashtag_id in hashtag_ids:
            hashtag_tag = hashtags_insert_arr[hashtag_insert_arr_key]
            hashtags_id_arr[hashtag_tag] = hashtag_id
            hashtag_insert_arr_key += 1

    if conversation_hashtags_insert_count > 0:
        for conversation_hashtags_insert_arr_id in range(conversation_hashtags_insert_count):
            hashtag_tag = conversation_hashtags_insert_arr[2*conversation_hashtags_insert_arr_id + 1]
            hashtag_id = hashtags_id_arr[hashtag_tag]
            conversation_hashtags_insert_arr[2*conversation_hashtags_insert_arr_id + 1] = hashtag_id

        conversation_hashtags_tuple = tuple(conversation_hashtags_insert_arr)
        cursor.execute(conversation_hashtags_sql_query, conversation_hashtags_tuple)

    if annotations_insert_count > 0:
        annotations_tuple = tuple(annotations_insert_arr)
        cursor.execute(annotations_sql_query, annotations_tuple)

    if links_insert_count > 0:
        links_tuple = tuple(links_insert_arr)
        cursor.execute(links_sql_query, links_tuple)

    if conversation_references_insert_count > 0:
        conversation_references_tuple = tuple(conversation_references_insert_arr)
        cursor.execute(conversation_references_sql_query, conversation_references_tuple)

    if context_entities_insert_count > 0:
        context_entities_tuple = tuple(context_entities_insert_arr)
        cursor.execute(context_entities_sql_query, context_entities_tuple)

    if context_domains_insert_count > 0:
        context_domains_tuple = tuple(context_domains_insert_arr)
        cursor.execute(context_domains_sql_query, context_domains_tuple)

    if context_annotations_insert_count > 0:
        context_annotations_tuple = tuple(context_annotations_insert_arr)
        cursor.execute(context_annotations_sql_query, context_annotations_tuple)

    if authors_insert_count > 0:
        authors_tuple = tuple(authors_insert_arr)
        cursor.execute(authors_sql_query, authors_tuple)

    if conversations_insert_count > 0:
        conversations_tuple = tuple(conversations_insert_arr)
        cursor.execute(conversations_sql_query, conversations_tuple)
        connection.commit()

    print_execution_time(start_time, cur_start_time, conversations_output)


def insert_conversations():
    print("=======================")
    print("INSERTING CONVERSATIONS")
    print("=======================")

    start_time = time.time()

    conversations_output = open('conversations.csv', 'w')

    writer = csv.writer(conversations_output)
    conversations_output_header = ["datum; celkovy cas; aktualny_cas"]
    writer.writerow(conversations_output_header)

    conversation_lines = []
    conversation_lines_count = 0
    for conversation_line in open('conversations.jsonl', 'r'):
        conversation_lines.append(conversation_line)
        conversation_lines_count += 1

        if conversation_lines_count == LINES_PER_PROC:
            proc_insert_conversations([conversation_lines, conversations_output, start_time])

            conversation_lines_count = 0
            conversation_lines = []

    if conversation_lines_count > 0:
        proc_insert_conversations([conversation_lines, conversations_output, start_time])

    conversations_output.close()


if __name__ == '__main__':
    connection_string = "dbname=twitter user=postgres password=postgres"
    connection = psycopg2.connect(connection_string)
    cursor = connection.cursor()

    create_tables()
    # insert_authors()
    insert_conversations()
    # alter_tables()

    connection.close()
    cursor.close()
