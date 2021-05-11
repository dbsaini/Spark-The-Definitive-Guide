# Databricks notebook source
# Push mtcars dataset to Spark
sc <- sparklyr::spark_connect(method = "databricks")

mtcars_sdf <- sparklyr::sdf_copy_to(sc, mtcars, overwrite = T)

# Output schema
schema <- list(cyl = "double",
               term = "string",
               estimate = "double",
               std_error = "double",
               statistic = "double",
               p_value = "double")

## Add a new column for each group and return the results
results_sdf <- sparklyr::spark_apply(mtcars_sdf,
                                  group_by = "cyl",
                                  function(e){
                                    # 'e' is a data.frame containing all the rows for each distinct UniqueCarrier
                                    tidymod <- broom::tidy(lm(mpg ~ ., data = e[, -2]))
                                    tidymod
                                  }, 
                                   # Specify schema
                                   columns = schema,
                                   # Do not copy packages to each worker
                                   packages = F)
head(results_sdf, 10)

# COMMAND ----------

summary(mtcars)

# COMMAND ----------

mtcars$adjustedwt = mtcars$wt * 10

# COMMAND ----------

str(mtcars)

# COMMAND ----------

