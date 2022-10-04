import psycopg2
import json
import time
import datetime
import multiprocessing


def create_tables():
    print("===============")
    print("CREATING TABLES")
    print("===============")

    start_time = time.time()
    cur_start_time = start_time

    connection = psycopg2.connect(connection_string)
    cursor = connection.cursor()

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
        tmp_id BIGSERIAL PRIMARY KEY,
        id INT8 NOT NULL,
        name VARCHAR(255) NOT NULL,
        description TEXT
    )
    """)

    cursor.execute("""
    DROP TABLE IF EXISTS context_entities
    """)
    cursor.execute("""
    CREATE TABLE context_entities(
        tmp_id BIGSERIAL PRIMARY KEY,
        id INT8 NOT NULL,
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
        id BIGSERIAL PRIMARY KEY NOT NULL,
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
        tmp_id BIGSERIAL PRIMARY KEY,
        process_order INT8 NOT NULL,
        id INT8 NOT NULL,
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
        tmp_id BIGSERIAL PRIMARY KEY,
        process_order INT8 NOT NULL,
        id INT8,
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

    cursor.close()
    connection.close()

    header = "Main process created empty tables in twitter database"
    print_execution_time(header, start_time, cur_start_time)


def alter_tables():
    print("===============")
    print("ALTERING TABLES")
    print("===============")

    start_time = time.time()
    cur_start_time = start_time

    connection = psycopg2.connect(connection_string)
    cursor = connection.cursor()

    # cursor.execute("""
    # DELETE FROM authors
    # WHERE tmp_id IN (
    #     SELECT a1.tmp_id FROM authors a1
    #     INNER JOIN (
    #         SELECT id, COUNT(id), MIN(process_order) AS min_process_order
    #         FROM authors
    #         GROUP BY id
    #         HAVING COUNT(id) > 1
    #     ) a2 ON a2.id = a1.id AND a2.min_process_order != a1.process_order
    # )
    # """)
    # cursor.execute("""
    # ALTER TABLE authors
    #     DROP CONSTRAINT authors_pkey,
    #     ADD PRIMARY KEY (id)
    #     DROP COLUMN tmp_id,
    # """)
    #
    # cursor.execute("""
    # DELETE FROM conversations
    # WHERE tmp_id IN (
    #     SELECT c1.process_order FROM conversations c1
    #     INNER JOIN (
    #         SELECT id, COUNT(id), MIN(process_order) AS min_process_order
    #         FROM conversations
    #         GROUP BY id
    #         HAVING COUNT(id) > 1
    #     ) c2 ON c2.id = c1.id AND c2.min_process_order != c1.process_order
    # )
    # """)
    # cursor.execute("""
    # ALTER TABLE conversations
    #     DROP CONSTRAINT conversations_pkey,
    #     ADD PRIMARY KEY (id)
    #     DROP COLUMN tmp_id,
    # """)
    #
    # cursor.execute("""
    # DELETE FROM hashtags
    # WHERE id IN (
    #     SELECT h1.id FROM hashtags h1
    #     INNER JOIN (
    #         SELECT tag, COUNT(tag), MIN(id) AS min_id
    #         FROM hashtags
    #         GROUP BY tag
    #         HAVING COUNT(tag) > 1
    #     ) h2 ON h2.tag = h1.tag AND h2.min_id != h1.id
    # )
    # """)
    # cursor.execute("""
    # ALTER TABLE hashtags
    #     ADD UNIQUE(tag)
    # """)
    #
    # connection.commit()

    cursor.execute("""
    SELECT id, tag
    FROM hashtags
    """)

    hashtag_rows = cursor.fetchall()
    for hashtag_row in hashtag_rows:
        if hashtag_row is None:
            break

        cursor.execute("""
        UPDATE conversation_hashtags
        SET hashtag_id = %s
        WHERE tmp_hashtag = %s
        """, (hashtag_row[0], hashtag_row[1]))

    # cursor.execute("""
    # ALTER TABLE conversation_hashtags
    #     DROP COLUMN tmp_hashtag,
    # """)

    connection.commit()
    cursor.close()
    connection.close()

    header = "Main process altered tables in twitter database"
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
    author_lines, process_order, start_time, connection_string = args

    cur_start_time = time.time()

    connection = psycopg2.connect(connection_string)
    cursor = connection.cursor()

    authors_id_arr = []
    authors_insert_count = 0
    authors_insert_arr = []
    authors_sql_query = """
    INSERT INTO 
        authors (process_order, id, name, username, description, followers_count, following_count, tweet_count, listed_count)
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

    author_lines_count = 0
    for author_line in author_lines:
        author_lines_count += 1
        author = json.loads(author_line)
        author_public_metrics = author["public_metrics"]

        author_id = author["id"]
        if author_id in authors_id_arr:
            continue
        authors_id_arr.append(author_id)

        name = author["name"].replace("\x00", "\uFFFD")[:255]
        username = author["username"].replace("\x00", "\uFFFD")[:255]
        description = author["username"].replace("\x00", "\uFFFD")
        followers_count = author_public_metrics["followers_count"]
        following_count = author_public_metrics["following_count"]
        tweet_count = author_public_metrics["tweet_count"]
        listed_count = author_public_metrics["listed_count"]

        authors_insert_arr.extend([process_order, author_id, name, username, description,
                                   followers_count, following_count, tweet_count, listed_count])

        if authors_insert_count > 0:
            authors_sql_query += ", (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        else:
            authors_sql_query += "(%s, %s, %s, %s, %s, %s, %s, %s, %s)"

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
        authors_tuple = tuple(authors_insert_arr)
        cursor.execute(authors_sql_query, authors_tuple)
        connection.commit()

    header = "Process {} processed {} author lines".format(multiprocessing.current_process().pid, author_lines_count)
    print_execution_time(header, start_time, cur_start_time)

    cursor.close()
    connection.close()


def insert_authors():
    print("=================")
    print("INSERTING AUTHORS")
    print("=================")

    pool = multiprocessing.Pool()

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

    if author_lines_count > 0:
        author_lines_arr.append(author_lines)

    process_order = 0
    args = []
    for author_lines in author_lines_arr:
        args.append((author_lines, process_order, start_time, connection_string))
        process_order += 1

    header = "Main process read all lines from author.jsonl file and split them amongst processes"
    print_execution_time(header, start_time, cur_start_time)

    for author_lines in author_lines_arr:
        proc_insert_authors([author_lines, process_order, start_time, connection_string])

    # results = pool.map(proc_insert_authors, args)


def proc_insert_conversations(args):
    conversation_lines, process_order, start_time, connection_string = args

    cur_start_time = time.time()

    connection = psycopg2.connect(connection_string)
    cursor = connection.cursor()

    conversations_id_arr = []
    conversations_insert_count = 0
    conversations_insert_arr = []
    conversations_sql_query = """
    INSERT INTO 
        conversations (process_order, id, author_id, content, possibly_sensitive, language, source, 
        retweet_count, reply_count, like_count, quote_count, created_at)
    VALUES
    """

    hashtags_dict = {}
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

    context_annotations_conversation_id_arr = []
    context_annotations_insert_count = 0
    context_domains_insert_arr = []
    context_entities_insert_arr = []
    context_domains_sql_query = """
    INSERT INTO
        context_domains (id, name, description)
    VALUES
    """
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
        conversations_id_arr.append(conversation_id)

        public_metrics = conversation["public_metrics"]
        author_id = conversation["author_id"]
        content = conversation["text"]
        possibly_sensitive = conversation["possibly_sensitive"]
        language = conversation["lang"]
        source = conversation["source"]
        retweet_count = public_metrics["retweet_count"]
        reply_count = public_metrics["reply_count"]
        like_count = public_metrics["like_count"]
        quote_count = public_metrics["quote_count"]
        created_at = conversation["created_at"]

        conversations_insert_arr.extend([process_order, conversation_id, author_id, content, possibly_sensitive, language, source,
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

                    if tag in hashtags_dict.keys():
                        continue
                    hashtags_dict[tag] = 0

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
                    domain_name = domain["name"]
                    domain_description = None
                    try:
                        domain_description = domain["description"]
                    except KeyError:
                        pass

                    context_domains_insert_arr.extend([domain_id, domain_name, domain_description])
                    if context_annotations_insert_count > 0:
                        context_domains_sql_query += ", (%s, %s, %s)"
                    else:
                        context_domains_sql_query += "(%s, %s, %s)"

                    entity = context_annotation["entity"]

                    entity_id = entity["id"]
                    entity_name = entity["name"]
                    entity_description = None
                    try:
                        entity_description = entity["description"]
                    except KeyError:
                        pass

                    context_entities_insert_arr.extend([entity_id, entity_name, entity_description])
                    if context_annotations_insert_count > 0:
                        context_entities_sql_query += ", (%s, %s, %s)"
                    else:
                        context_entities_sql_query += "(%s, %s, %s)"

                    context_annotations_conversation_id_arr.append(conversation_id)

                    context_annotations_insert_count += 1

        if conversations_insert_count > 0:
            conversations_sql_query += ", (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        else:
            conversations_sql_query += "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        conversations_insert_count += 1

    if hashtags_insert_count > 0:
        hashtags_sql_query += "ON CONFLICT DO NOTHING"
        hashtags_tuple = tuple(hashtags_insert_arr)
        cursor.execute(hashtags_sql_query, hashtags_tuple)

    if conversation_hashtags_insert_count > 0:
        select_hashtags_id_query_args = []
        select_hashtags_id_query = "SELECT id, tag FROM hashtags WHERE tag IN ("

        for hashtag_tag in hashtags_dict.keys():
            select_hashtags_id_query += "%s,"
            select_hashtags_id_query_args.append(hashtag_tag)
        select_hashtags_id_query = select_hashtags_id_query[:-1]
        select_hashtags_id_query += ")"

        cursor.execute(select_hashtags_id_query, tuple(select_hashtags_id_query_args))
        hashtag_id_rows = cursor.fetchall()

        for hashtag_id_row in hashtag_id_rows:
            hashtags_dict[hashtag_id_row[1]] = hashtag_id_row[0]

        conversation_hashtags_insert_key = 0
        for conversation_hashtag_insert_val in conversation_hashtags_insert_arr:
            if (conversation_hashtags_insert_key % 2) == 1:
                conversation_hashtags_insert_arr[conversation_hashtags_insert_key] \
                    = hashtags_dict[conversation_hashtag_insert_val]
            conversation_hashtags_insert_key += 1

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

    if context_annotations_insert_count > 0:
        context_domains_sql_query += " returning id"
        context_domains_tuple = tuple(context_domains_insert_arr)
        cursor.execute(context_domains_sql_query, context_domains_tuple)
        context_domains_rows = cursor.fetchall()

        context_entities_sql_query += " returning id"
        context_entities_tuple = tuple(context_entities_insert_arr)
        cursor.execute(context_entities_sql_query, context_entities_tuple)
        context_entities_rows = cursor.fetchall()

        context_annotations_query = """
        INSERT INTO
            context_annotations(conversation_id, context_domain_id, context_entity_id)
        VALUES
        """
        context_annotations_arr = [0 for _ in range(context_annotations_insert_count*3)]
        for i in range(context_annotations_insert_count):
            context_annotations_arr[3 * i] = context_annotations_conversation_id_arr[i]
            context_annotations_arr[3 * i + 1] = context_domains_rows[i][0]
            context_annotations_arr[3 * i + 2] = context_entities_rows[i][0]
            if i > 0:
                context_annotations_query += ", (%s, %s, %s)"
            else:
                context_annotations_query += "(%s, %s, %s)"
        context_annotations_tuple = tuple(context_annotations_arr)

        cursor.execute(context_annotations_query, context_annotations_tuple)

    if conversations_insert_count > 0:
        conversations_tuple = tuple(conversations_insert_arr)
        cursor.execute(conversations_sql_query, conversations_tuple)

        connection.commit()
        header = "Process {} processed {} conversation lines".format(multiprocessing.current_process().pid,
                                                                     conversation_lines_count)
        print_execution_time(header, start_time, cur_start_time)

    cursor.close()
    connection.close()


LINES_PER_PROC = 10000
LINES_PER_READ = 500


def insert_conversations():
    print("=======================")
    print("INSERTING CONVERSATIONS")
    print("=======================")

    pool = multiprocessing.Pool()

    start_time = time.time()
    cur_start_time = start_time

    conversation_lines_arr_count = 0
    conversation_lines_arr = []
    conversation_lines = []
    conversation_lines_count = 0
    process_order = 0
    for conversation_line in open('conversations.jsonl', 'r'):
        conversation_lines.append(conversation_line)
        conversation_lines_count += 1

        if conversation_lines_count == LINES_PER_PROC:
            conversation_lines_arr_count += 1
            conversation_lines_arr.append(conversation_lines)

            conversation_lines_count = 0
            conversation_lines = []

            if conversation_lines_arr_count == LINES_PER_READ:
                args = []
                for conversation_lines in conversation_lines_arr:
                    args.append((conversation_lines, process_order, start_time, connection_string))
                    process_order += 1

                header = "Main process read {} * {} lines from conversations.jsonl file and" \
                         " split them amongst processes".format(LINES_PER_READ, LINES_PER_PROC)
                print_execution_time(header, start_time, cur_start_time)

                for conversation_lines in conversation_lines_arr:
                    proc_insert_conversations([conversation_lines, process_order, start_time, connection_string])

                conversation_lines_arr_count = 0
                conversation_lines_arr = []

                # results = pool.map(proc_insert_conversations, args)

                cur_start_time = time.time()

    if conversation_lines_count > 0:
        conversation_lines_arr.append(conversation_lines)

        conversation_lines_count = 0
        conversation_lines = []

    if conversation_lines_arr_count > 0:
        args = []
        for conversation_lines in conversation_lines_arr:
            args.append((conversation_lines, process_order, start_time, connection_string))
            process_order += 1

        header = "Main process read {} * {} lines from conversations.jsonl file and" \
                 " split them amongst processes".format(conversation_lines_arr_count, LINES_PER_PROC)
        print_execution_time(header, start_time, cur_start_time)

        conversation_lines_arr_count = 0
        conversation_lines_arr = []

        # results = pool.map(proc_insert_conversations, args)


if __name__ == '__main__':
    connection_string = "dbname=twitter user=postgres password=postgres"

    create_tables()
    insert_authors()
    # insert_conversations()
    # alter_tables()
