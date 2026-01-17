# 🎬 Movie Recommendation System Backend

A backend application for a movie recommendation system based on PostgreSQL and Collaborative Filtering algorithms. This project implements a complete data pipeline from raw data management to personalized ranking delivery.

## 📌 Project Overview
- **Project Type:** Graduate School Team Project
- **Role:** **Pipeline Architect** & Backend Developer
- **Tech Stack:** Python, PostgreSQL, Pandas, Scikit-learn
- **Key Implementation:** Collaborative Filtering, Content-based Filtering, SQL Optimization

## 📂 Project Documents
You can view the detailed project report and presentation slides below.
*(Please click the links to preview the PDF files)*

* 📄 **Project Report:** [View Report PDF](./docs/TermProject_team6.pdf)
* 📊 **Presentation Slides:** [View Slides PDF](./docs/Team6_20240613.pdf)

## 🛠 My Main Contributions

I was responsible for the **core backend infrastructure and application logic**, excluding the specific recommendation algorithms (Cold Start/Filtering).

### 1. System Architecture & Flow Control
* **Application Logic:** Designed the main execution flow (`main.py`) to manage user sessions and route inputs to appropriate database operations.
* **Modularity:** Decoupled database connection logic from the business logic for better maintainability and scalability.

### 2. User & Data Management
* **User Authentication:** Implemented secure `signup` and `signin` processes to manage user sessions and identities.
* **Interaction Logging:** Developed functions to record user activities, such as **Adding View Counts** and **Registering Ratings** in real-time, which serve as the foundation for the recommendation engine.
* **Search & Retrieval:** Built efficient SQL queries to search movies by keywords and retrieve detailed metadata.

### 3. Ranking System Implementation
* **Statistical Ranking:** Engineered algorithms to generate top lists based on user logs:
    * **Ranking by Views:** Aggregates view counts to display trending movies.
    * **Ranking by Ratings:** Calculates average scores to display high-quality movies.

---

## 🧩 Other Features (Team Effort)
* **Recommendation Engine:** Implemented by team members, utilizing the data pipeline I built to provide Content-Based and Collaborative Filtering recommendations.

## 📁 Repository Structure
```text
movie-recommendation
 ├── data/            # Contains placeholder files for input structure
 │    ├── movies.txt
 │    ├── users.txt
 │    └── ratings.txt
 ├── docs/            # Project documentation (Report & Slides)
 ├── src/
 │    ├── main.py     # Main application entry point
 │    └── operations.py # Database operations & logic
 ├── .gitignore
 ├── requirements.txt
 └── README.md