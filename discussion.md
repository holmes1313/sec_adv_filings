## 5. Scalability and Performance (Discussion - Estimated Time: 15–30 minutes)

**Task**: Optimize the data pipeline for performance.  
**Details**: Write a brief explanation of how you would handle scalability and performance issues if the dataset were significantly larger.

1. **Chunk Processing**  
   Prevents memory overload when reading large datasets. Use `pandas.read_csv(..., chunksize=N)` to process data in smaller, manageable chunks.

2. **Vectorized Operations in Pandas**  
   Loops in Pandas are slow; vectorized operations (`.apply()`, `.groupby()`, `.transform()`) are significantly faster and more memory-efficient.

3. **Database Indexing**  
   Indexing accelerates SQL queries, especially when filtering, joining, or aggregating large tables. Add indexes to frequently queried fields like `FirmCrdNb`.

4. **Adopt Distributed Processing Using Spark**  
   Apache Spark handles massive datasets across distributed systems, ideal for batch processing, transformation, and analytics pipelines. Use PySpark for seamless Python integration.



## 7. Automated Testing and Data Quality (Discussion - Estimated Time: 15–30 minutes)

**Task**: Write a brief explanation of how you would implement automated testing and ensure data quality.

- **Automated Testing**  
  Use `pytest` to write unit tests for the parser (`extract_private_funds()`, `parse_pdf()`) and analysis logic (top fund selection, plotting, etc.). Include integration tests for database interactions and full pipeline validation.

- **Data Validation**  
  Ensure all required fields (e.g., fund name, ID, gross asset value
