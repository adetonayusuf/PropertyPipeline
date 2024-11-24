# PropertyPipeline
Real Estate ETL Pipeline

Zipco Real Estate Agency operates in the fast-paced and
competitive world of real estate, where timely access to accurate
information is crucial for success. .
Our success factors likely include a strong understanding of local
market dynamics, effective marketing strategies, and a
commitment to client relationships. Their business focus may
center on providing exceptional customer service, leveraging
technology for efficient operations, and maintaining a robust
online presence to attract leads.

However, the company is currently facing a significant
data challenge that hinders its operational efficiency.
The existing data processing workflow is inefficient,
resulting in disparate datasets.

At Zipco Real Estate Agency, we encounter several pressing challenges within our data processing:
- Inefficient Data Processing Workflow
- Increased Operational Costs
- Disparate Datasets and Inconsistent format
- Compromised Data Quality

Rationale for the Project

Implementing a comprehensive ETL (Extract, Transform, Load) pipeline at Zipco Real Estate Agency is multifaceted, addressing the core
challenges the company faces while also aligning with its strategic goals, and the desire to overcome existing data challenges, enhance
operational efficiency, and position the company for sustainable growth and success in a competitive landscape.
1. Enhanced Operational Efficiency: By automating and streamlining data processing workflows, the ETL pipeline will significantly reduce
the time and effort required to gather, clean, and prepare data.
2. Improved Data Quality and Consistency: The ETL process will standardize data formats and ensure that information from various
sources is accurately integrated. This consistency enhances data quality, enabling agents and management to make informed decisions
based on reliable and up-to-date information.
3. Timely Access to Critical Information: With a well-structured ETL pipeline, Zipco will be able to access critical property information
and market insights in real-time. This timely access is essential for making quick decisions in a fast-paced real estate environment, ultimately
leading to better service for clients and increased sales opportunities.
4. Cost Reduction: By minimizing manual data handling and reducing errors, the ETL pipeline can lead to significant cost savings. Lower
operational costs can be redirected towards growth initiatives, marketing efforts, or enhancing customer service, thereby improving overall
profitability.
5. Competitive Advantage: In the competitive real estate market, having access to high-quality, timely data can set Zipco apart from its
competitors. By leveraging advanced data management capabilities, the agency can offer superior insights to clients, enhance marketing
strategies, and respond more effectively to market trends.
6. Enhanced Decision-Making: With improved data quality and accessibility, management will be better equipped to make strategic
decisions based on accurate insights and analytics. This informed decision-making can drive the agency's growth and help it navigate the
complexities of the real estate market more effectively.

Aim of Project

- Data Extraction
- Data Cleaning and Transformation
- Database Loading
- Automation

![Data Atchitecture](https://github.com/adetonayusuf/PropertyPipeline/blob/main/Data%20Architecture2.png)

Tools & Technologies

- Python
- Postgres
- Power BI
- Window Task Scheduler

  I got the real estate API from Realty Mole Property then I extracted and transformed via Python and loaded the transformed data into Postgress. Check link below to view the pipeline code, this solve Zipco Real Estate Agency problem as stated below
    - Inefficient Data Processing Workflow - With this pipeline data is extracted and processed within seconds which makes information available for Zipco management to
      make effective decision on a timely basis.

    - Increased Operational Costs - The pipeline will reduce the operational costs especially relating to data processing by atleast 90%.
 
    - Disparate Datasets and Inconsistent format - The dataset in the pipeline is well cleaned and transformed. The format is well formatted and fit for decision making.
 
    - Compromised Data Quality - The data quality has been improved by at least 95%

  [Postgres Pipeline](https://github.com/adetonayusuf/PropertyPipeline/blob/main/postgres_pipeline.py)

  Below is the ERD from Schema in Postgres that the automated flow from the pipeline into Postgres

  ![ERD from Schema](https://github.com/adetonayusuf/PropertyPipeline/blob/main/ERD%20for%20Schema%20for%20Postgres%20SQL.png)

  Below is the data modeling

  ![Data Modelling](https://github.com/adetonayusuf/PropertyPipeline/blob/main/Data%20Modelling.jpeg)

  I amported the data directly from Postgres to Power BI to building sales performance dashboard to show how much property was sold as at when and the location of each property sold and some other metrics were considered. Below is the dashboard
  ![Zipco Property Sales Dashboard](https://github.com/adetonayusuf/PropertyPipeline/blob/main/Zipco%20Real%20Estate%20Agency%20Dashboard.png)

  I automated the refresh process using Task Schedular, this task will be run on a weekly basis to run the pipeline and also update the power Bi, so as to get the updated report on a weekly basis.
  ![Task Scheduler](https://github.com/adetonayusuf/PropertyPipeline/blob/main/Task%20Scheduler.png)
