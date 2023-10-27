def task_1a(df: "pyspark.sql.dataframe.DataFrame", 
            F: "pyspark.sql.functions") -> "pyspark.sql.dataframe.DataFrame":
    # Place your code to transform DaraFrame here
    # Filed id indicates post id.
    modified_df = df.select( F.col("id").alias("post_id"), F.col("likes.count").alias("likes_count")
        ).orderBy( F.col("likes_count").desc(), F.col("post_id").asc()
    )
    
    return modified_df

def task_1b(df: "pyspark.sql.dataframe.DataFrame", 
            F: "pyspark.sql.functions") -> "pyspark.sql.dataframe.DataFrame":
    # Place your code to transform DaraFrame here
    modified_df = df.select( F.col("id").alias("post_id"), F.col("comments.count").alias("comments_count")
        ).orderBy( F.col("comments_count").desc(), F.col("post_id").asc()
    )
    
    return modified_df


def task_1c(df: "pyspark.sql.dataframe.DataFrame", 
            F: "pyspark.sql.functions") -> "pyspark.sql.dataframe.DataFrame":
    # Place your code to transform DaraFrame here
    modified_df = df.select(F.col("id").alias("post_id"), F.col("reposts.count").alias("reposts_count")
        ).orderBy(F.col("reposts_count").desc(), F.col("post_id").asc()
    )
    
    
    return modified_df


def task_2a(df: "pyspark.sql.dataframe.DataFrame", 
            F: "pyspark.sql.functions") -> "pyspark.sql.dataframe.DataFrame":
    # Place your code to transform DaraFrame here
    # Look through df
    modified_df = df.groupby("ownerId")\
        .agg(F.count("ownerId").name("count"))\
        .orderBy(F.col("count").desc(), F.col("ownerId").asc()
    )
    
    return modified_df


def task_2b(df: "pyspark.sql.dataframe.DataFrame", 
            F: "pyspark.sql.functions") -> "pyspark.sql.dataframe.DataFrame":
    # Place your code to transform DaraFrame here 
    modified_df = df.where(F.col("copy_history").isNotNull())\
                    .select( F.col("owner_id"), F.col("copy_history.id").name("src_post_id"),
                    ).groupBy("owner_id").agg( F.count("src_post_id").name("count"))\
                        .orderBy( F.col("count").desc(),F.col("owner_id").asc()
                    )
    
    return modified_df


def task_3(df: "pyspark.sql.dataframe.DataFrame", 
           F: "pyspark.sql.functions") -> "pyspark.sql.dataframe.DataFrame":
    # Place your code to transform DaraFrame here

    modified_df = df.where(F.col("copy_history").isNotNull())\
                    .select(
                        F.col("id").alias("user_post_id"),
                        F.col("copy_history.id").getItem(0).alias("group_post_id"))\
                    .where(F.col("copy_history.owner_id").getItem(0) == -94)\
                    .groupby("group_post_id")\
                    .agg(F.sort_array(F.collect_list('user_post_id')).alias('user_post_ids')) \
                    .withColumn("reposts_count", F.size("user_post_ids"))\
                    .orderBy(
                        F.col("reposts_count").desc(),
                        F.col("group_post_id").asc()
                    ) # .show(10)
    
    
    return modified_df

def task_4(df: "pyspark.sql.dataframe.DataFrame",
           F: "pyspark.sql.functions",
           T: "pyspark.sql.types",
           emojis_data: dict,
           broadcast_func: "spark.sparkContext.broadcast") -> 'Tuple["pyspark.sql.dataframe.DataFrame"]':
    # You are able to modify any code inside this function.
    # Only 'emoji' package import is allowed for this task.
    
    import emoji
    reg_exp = emoji.get_emoji_regexp()

    # 0) Helpers
    @F.udf(returnType=T.ArrayType(T.StringType()))
    def getEmojiSlice(text):
        emojis = []
        for reg_match in reg_exp.finditer(text):
            emojis.append(reg_match.group())
        return emojis

    
    broarcasted_dict = broadcast_func(emojis_data)
    @F.udf(returnType=T.StringType())
    def getSentiment(col):
        return broarcasted_dict.value.get(col, None)

    
    # 1) Gather emoji's from df
    raw_text_emoji_df = df.where(F.col("text").isNotNull())\
                            .select(
                                F.col("id"), 
                                F.col("text")
                            )\
                            .withColumn("emojis", getEmojiSlice("text"))\
                            .where(F.size("emojis") > 0)\
                            .select(F.col("id").name("postID"), F.col("emojis"))

    # 2) Get all emojis counted
    emoji_df = raw_text_emoji_df.select(F.explode("emojis").name("emoji"))\
                    .groupBy("emoji")\
                    .agg(F.count("emoji").name("count"))\
                    .withColumn("sentiment", getSentiment(F.col("emoji")))

    positive_emojis = emoji_df.where(F.col("sentiment") == "positive")\
                .select(
                    F.col("emoji"),
                    F.col("count"))\
                .orderBy(F.col("count").desc())
    
    neutral_emojis = emoji_df.where(F.col("sentiment") == "neutral")\
                .select(
                    F.col("emoji"),
                    F.col("count"))\
                .orderBy(F.col("count").desc())
    
    negative_emojis = emoji_df.where(F.col("sentiment") == "negative")\
                .select(
                    F.col("emoji"),
                    F.col("count"))\
                .orderBy(F.col("count").desc())
    
    return (positive_emojis, neutral_emojis, negative_emojis)


def task_5(df: "pyspark.sql.dataframe.DataFrame",
           F: "pyspark.sql.functions",
           W: "pyspark.sql.window.Window",
           top_n_likers: int) -> "pyspark.sql.dataframe.DataFrame":
    # Place your code to transform DaraFrame here
    
    
    modified_df = df.groupBy(["ownerId", "likerId"])\
                    .agg(F.count("ownerId").name("count"))
    
    # removing self likes
    modified_df = modified_df.where(modified_df.ownerId != modified_df.likerId)
    
    # using window function
    windowSpec  = W.partitionBy("ownerId")\
                    .orderBy(
                        F.col("ownerId").asc(), 
                        F.col("count").desc(), 
                        F.col("likerId").asc()
    )
    
    modified_df = modified_df.withColumn("row_number", F.row_number().over(windowSpec))
    
    modified_df = modified_df.where(modified_df.row_number <= top_n_likers)
    
    modified_df = modified_df.drop("row_number")
    
    modified_df = modified_df.orderBy( F.col("ownerId").asc(), F.col("count").desc(), F.col("likerId").asc()
    )
    
    return modified_df


def task_6(df: "pyspark.sql.dataframe.DataFrame",
           F: "pyspark.sql.functions",
           W: "pyspark.sql.window.Window") -> "pyspark.sql.dataframe.DataFrame":
    # Place your code to transform DaraFrame here    
    userA = df.groupBy(
                    F.col("likerId"),
                    F.col("ownerId"))\
                .agg(F.count("ownerId").name("count"))

    # Removing self likes
    userA = userA.where(userA.ownerId != userA.likerId)

    # SEARCH LIKES
    window = W.partitionBy("likerId").orderBy(
            # F.col("ownerId").asc(),
            F.col("count").desc(),
            F.col("likerId").asc()
    )


    userA = userA.withColumn("max_likes", F.max("count").over(window))\
                 .where(F.col("count") == F.col("max_likes"))\
                 .drop("max_likes") 
    # Left only top likers (with "count" == topG count within "ownerId")
    
    userB = userA.alias("userB")
    
    modified_df = userA.alias("userA").join(
                                            userB, \
                                            (F.col("userA.ownerId") == F.col("userB.likerId")) &
                                            (F.col("userA.likerId") == F.col("userB.ownerId")) &
                                            (F.col("userA.ownerId") < F.col("userB.ownerId")),
                                            "inner")\
                                        .select(
                                            F.col("userA.ownerId").name("user_a"),
                                            F.col("userB.ownerId").name("user_b"),
                                            F.col("userB.count").name("likes_from_a"),
                                            F.col("userA.count").name("likes_from_b")
                                        )

    modified_df = modified_df.withColumn("mutual_likes", modified_df.likes_from_b + modified_df.likes_from_a)

    # Removing identical (user_a = 123, user_b = 456 and user_a = 456, user_b = 123) entries
    modified_df = modified_df.withColumn(
        "sorted_ids",
        F.concat(
            F.least(F.col("user_a"), F.col("user_b")),
            F.lit("_"),
            F.greatest(F.col("user_a"), F.col("user_b"))
        )
    )

    modified_df = modified_df.dropDuplicates(["sorted_ids"])

    modified_df = modified_df.select( F.col("user_a"), F.col("user_b"), F.col("likes_from_a"), F.col("likes_from_b"), F.col("mutual_likes")
    )

    modified_df = modified_df.sort(F.col("mutual_likes").desc(), F.col("user_a").asc(), F.col("user_b").asc())
    
    return modified_df