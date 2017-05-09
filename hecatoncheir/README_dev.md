
# Adding support for new DBMS

To add support for new DBMS, you need to implement two modules:
NewDBMSDriver.py and NewDBMSProfiler.py.


## NewDBMSDriver

NewDBMSDriver.py must be derived from the DbDriverBase class, and it
needs to implement following 5 methods as the public inteface:

* __init__()
* connect(self)
* query_to_resultset()
* q2rs()
* disconnect()

To implement those functions, you can call some common functions in
the DbDriverBase class which can be shared by the multiple DBMSes.


## NewDBMSProfiler

NewDBMSProfiler.py must be derived from the DbDriverProfler class, and
it needs to implement following 14 methods as the public interface.

* __init__()
* get_schema_names()
* get_table_names()
* get_column_names()
* get_sample_rows()
* get_column_datatypes()
* get_row_count()
* get_column_nulls()
* has_minmax()
* get_column_min_max()
* get_column_most_freq_values()
* get_column_least_freq_values()
* get_column_cardinalities()
* run_record_validation()

To implement those functions, you can call some common functions in
the DbProfilerBase class which can be shared by the multiple DBMSes.


## Error Handling

NewDBMSProfiler must catch those native exceptions (DatabaseError) not
to go out of the profiler module. Instead, catch the native exceptions
and throw one of the exceptions in the DbProfilerException
module. This is because of supporting multiple DBMSes.

```
        except self.dbdriver.driver.DatabaseError as e:
            raise QueryError("Query Error", query=q, source=e)
```


## Unit tests

Please keep in mind that your new driver and profiler must have the
dedicated unit tests.


## Reference implementation

Please take a look at ./oracle or ./pgsql directory for the reference
implementation.


EOF
