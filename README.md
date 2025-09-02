🧠 Task_04_Descriptive_Stats

A cross-strategy data summarization toolkit designed to explore and describe real-world datasets — with or without third-party libraries — as part of an applied research challenge on 2024 U.S. presidential election social media data.

📌 Project Overview

This project provides a flexible and extensible framework for generating descriptive statistics, enabling researchers to quickly extract insights from datasets that may include nested JSON structures, inconsistent formatting, or mixed categorical and numeric fields.

To ensure transparency and reproducibility, the toolkit was implemented in three versions:

Pure Python (no third-party libraries)

Pandas-based implementation

Polars-based implementation

All versions can:

Unpack nested JSON-like fields

Compute descriptive statistics for numeric columns: count, mean, min, max, standard deviation

Compute descriptive statistics for non-numeric columns: count, unique values, and most frequent value

Optionally perform aggregation/grouping by one or more columns

This approach allows users to select the version best suited to dataset size, computational resources, or workflow preferences.

🚀 How to Run
Requirements

Python 3.10+

For pandas_stats.py:

pip install pandas


For polars_stats.py:

pip install polars


Optional visualization support:

pip install matplotlib seaborn plotly

Running Each Script

⚠️ Do not upload datasets in the repository.
Each script prompts the user to select a .csv file — no hardcoding needed.

Pure Python:

python pure_python_stats.py


Pandas:

python pandas_stats.py


Polars:

python polars_stats.py

📊 Script Capabilities

Each script:

Detects improperly formatted or nested columns dynamically

Unpacks JSON strings into analyzable columns

Performs summary statistics:

Numeric fields: count, mean, min, max, standard deviation

Non-numeric fields: count, unique values, most frequent value

Offers grouping by one or two columns

Displays results in a clean, side-by-side tabular format

📁 Folder Structure
.
├── pure_python_stats.py
├── pandas_stats.py
├── polars_stats.py
├── README.md
└── (No datasets included)

🔍 Key Insights & Learnings

Polars: Best performance on large datasets and nested JSON, especially with schema optimization

Pandas: Easiest for rapid prototyping, widely familiar

Pure Python: Reinforces fundamental programming concepts; slower but ideal for learning

LLM Assistance (ChatGPT-style models): Helpful for prototyping template logic; human judgment is essential for handling edge cases, designing CLI prompts, and ensuring flexibility across datasets

This project demonstrates a robust, reproducible, and flexible framework for descriptive statistics across multiple Python implementations, supporting both exploratory analysis and educational purposes.
