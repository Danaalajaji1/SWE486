from pyflink.table import EnvironmentSettings, TableEnvironment

# Set up the table environment
env_settings = EnvironmentSettings.new_instance().in_batch_mode().build()
table_env = TableEnvironment.create(env_settings)

# Create a Flink table to read all CSV files from a directory
table_env.execute_sql("""
     CREATE TABLE news_table (
        requested_url STRING,
        plain_text STRING,
        published_date STRING,
        title STRING,
        tags STRING,
        categories STRING,
        author STRING,
        sitename STRING,
        image_url STRING,
        `language` STRING,  
        language_score FLOAT,
        responded_url STRING,
        publisher STRING,
        warc_path STRING,
        crawl_date STRING
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file:///Users/sarah/Desktop/ccnews-dataset/',  -- Folder path without wildcard
        'format' = 'csv',
        'csv.ignore-parse-errors' = 'true',
        'csv.ignore-first-line' = 'true'
    )
""")

# Clean and process the data using SQL with TRY_CAST to handle potential parsing issues
cleaned_data = table_env.sql_query("""
    SELECT 
        LOWER(REGEXP_REPLACE(plain_text, '[^a-zA-Z0-9\\s]', '')) AS cleaned_plain_text,
        TRY_CAST(TO_TIMESTAMP(published_date, 'yyyy-MM-dd') AS TIMESTAMP) AS parsed_published_date,  -- convert string to timestamp
        title,
        tags,
        categories,
        author,
        sitename,
        `language`,
        language_score,
        publisher
    FROM news_table
    WHERE `language` IN ('ar', 'en') 
    AND published_date IS NOT NULL
""")

# Perform your analysis of publication volume by day
result_by_day = table_env.sql_query("""
    SELECT 
        DATE_FORMAT(parsed_published_date, 'yyyy-MM-dd') AS publication_day,
        COUNT(*) AS publication_count
    FROM (
        SELECT 
            LOWER(REGEXP_REPLACE(plain_text, '[^a-zA-Z0-9\\s]', '')) AS cleaned_plain_text,
            TRY_CAST(TO_TIMESTAMP(published_date, 'yyyy-MM-dd') AS TIMESTAMP) AS parsed_published_date,
            title,
            tags,
            categories,
            author,
            sitename,
            `language`,
            language_score,
            publisher
        FROM news_table
        WHERE `language` IN ('ar', 'en') 
        AND published_date IS NOT NULL
    )
    GROUP BY DATE_FORMAT(parsed_published_date, 'yyyy-MM-dd')
    ORDER BY publication_count DESC
    LIMIT 20
""")

# To show the result
print("Top 20 days with the highest volume of news publications:")
result_by_day.execute().print()

# Group the publications by the day of the week and count the number of publications
weekly_publication_counts = table_env.sql_query("""
    SELECT 
        DATE_FORMAT(parsed_published_date, 'EEEE') AS day_of_week,  -- Extract the day of the week
        COUNT(*) AS weekly_count
    FROM (
        SELECT 
            LOWER(REGEXP_REPLACE(plain_text, '[^a-zA-Z0-9\\s]', '')) AS cleaned_plain_text,
            TRY_CAST(TO_TIMESTAMP(published_date, 'yyyy-MM-dd') AS TIMESTAMP) AS parsed_published_date,
            title,
            tags,
            categories,
            author,
            sitename,
            `language`,
            language_score,
            publisher
        FROM news_table
        WHERE `language` IN ('ar', 'en') 
        AND published_date IS NOT NULL
    )
    GROUP BY DATE_FORMAT(parsed_published_date, 'EEEE')
    ORDER BY weekly_count DESC
""")

# Show the results for weekly publication counts
print("Weekly publication counts by day of the week:")
weekly_publication_counts.execute().print()

# Count the total number of rows in the cleaned dataset
result_iterator = cleaned_data.execute().collect()
row_count = sum(1 for _ in result_iterator)
print(f"Total rows in the cleaned DataFrame: {row_count}")