[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.x&color=orange)

## Elation Connector

## 🔗  Docs
Check out our [docs](https://thetuvaproject.com/) to learn about the Tuva Project and how you can use it on your healthcare data.
<br/><br/>

## 🧰  What does this repo do?
The Elation Connector is a dbt project that maps raw data from Elation to the Tuva Core Data Model and then builds all the clinical Tuva Data Marts.  Your Elation data should be organized according to this [data dictionary](https://dbdocs.io/hosteddb_support/hosted_database_snowflake).
<br/><br/>  

## 🔌 Database Support
- BigQuery
- Redshift
- Snowflake
<br/><br/>  

## ✅  Quickstart Guide

### Step 1: Pre-requisites
You must have medical record data from Elation in a data warehouse supported by this project.  Elation offers customers access to data via Snowflake data share.
<br/><br/> 

### Step 2: Configure Input Database and Schema
Next you need to tell dbt where your Elation source data is located.  Do this using the variables `input_database` and `input_schema` in the `dbt_project.yml` file.  You also need to configure your `profile` in the `dbt_project.yml`.
<br/><br/> 

### Step 4: dbt deps
Execute the command `dbt deps` to install The Tuva Project.  By default, this connector will use any version of the Tuva Project after 0.5.0 which is when clinical support was released.
<br/><br/>

### Step 4: Run
Now you're ready to run the connector and the Tuva Project.  For example, using dbt CLI you would `cd` to the project root folder in the command line and execute `dbt build`.  You're now ready to do clinical data analytics!  Check out the [data mart](https://thetuvaproject.com/data-marts/about) in our docs to learn what tables you should query.
<br/><br/>

## 🙋🏻‍♀️ How do I contribute?
Have an opinion on the mappings? Notice any bugs when installing and running the project?
If so, we highly encourage and welcome feedback!  Feel free to submit an issue or drop a message in Slack.
<br/><br/>

## 🤝 Join our community!
Join our growing community of healthcare data practitioners on [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)!
