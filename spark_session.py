from pyspark.sql import SparkSession
app = SparkSession.builder.appName("app0")
print(dir(app))
spark = SparkSession.builder.appName("app1").getOrCreate()
print(id(spark))
spark = SparkSession.builder.appName("app2").getOrCreate()
print(id(spark))

"""
select e.emp_id, avg(s.salary) as avg_salary
from employee as e
inner join salary as s on s.emp_id=e.emp_id
group by e.emp_id.
having s.salary > avg_salary
"""