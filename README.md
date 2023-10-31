[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/GhX0-lnL)
# Data and Programming for Public Policy II - Python Programming
# PPHA 30538


## Final Project: Reproducible Research
## Autumn 2023


## Due: Tuesday, December 6th by midnight on GitHub/Gradescope

### Project Description:
The goal of this project is to showcase your knowledge of Python by applying it to a research project of your own.  This is not a research methods course, so the quality of the
research is ancillary to the quality of your programming.  As a baseline your project should have the following major components:

  1. Data wrangling
  
You must use a minimum of three datasets, at least one of which should be retrieved automatically from the web using APIs or web scraping.  All processing of the data should be handled by your code, including all merging and reshaping.  Any automatic data retrieval must have an option to toggle accessing the web off if the data is already downloaded.  This is where you can showcase your abilities practiced in homework 1, and in Data and Programming 1.

  2. Plotting
  
From that data, you will create a minimum of two static plots using MatPlotLib/Pandas/Seaborn, and two interactive plots for Shiny.  Your Shiny does not have to be shared on shinyapps.io.  The skills used here will roughly correspond to your work on homework 2.

  3. Text processing

You will now introduce some form of text analysis, similar to that of homework 3.  While it should relate to your broader question, this may be distinct data from what you created in part 1, and the results of it may be used in your plotting or analysis.

  4. Analysis
  
Then you will fit a model to your data and report basic results.  As this is not a statistics or econometrics class, the model you choose and the validity of your results are not terribly important; fitting an OLS model with fixed effects that has insignificant p-values will not be penalized.  The goal is to show you can come up with a basic research question then prepare your data through the previous steps to have it ready for model fitting.

  5. Writeup
  
You will then spend *no more than 2-3 pages* writing up your project.  You should describe your research question, then discuss the approach you took and the coding involved, including discussing any weaknesses or difficulties encountered.  Finish with a brief discussion of results, and how this could be fleshed out in future research.  The primary purpose of this writeup is to inform me of what I am reading before I look at your code.

The top of your writeup should include the names of all your group members, and everyone's GitHub user IDs.

### Instructions
You may work on this project alone, or in [groups of up to three](https://github.blog/2018-03-06-how-to-use-group-assignments-in-github-classroom/).  All groups must be formed *on GitHub classrooms* before any work is done - it is not possible to join a group after.  If you work alone, then at least one of the first three parts above must be of a more advanced nature that exhibits your coding skills.  If you work in a group, then more of the parts must be advanced.  The exact nature of what makes something more advanced will be discussed in class; imagine, for example, the difference between standard matplotlib output with default settings, and output that has many elements adjusted to improve the display.  I strongly encourage you to discuss your project with me in advance.

It is required that you use GitHub, and I may use your past commits to understand your thought process for partial credit, and to monitor group participation.  You will not get full credit if your repository does not show multiple commits as you build your project, especially for groups.  Expectations for the scope of the project will be higher for groups than for individuals, and the division of labor will be up to each group.  While I will lean toward giving the same grade to everyone in a group, it is possible that group members will recieve different grades based on commit history.

Your final repository must contain the following: 
1. Your Python code and commit history.  Split your code up into multiple files named for the 4 categories above, plus one more for Shiny (app.py).
2. A folder named "data" that contains the initial, *unmodified* dataframes you download, (unless they are large, in which case they should be hosted on Drive or Dropbox with a link provided), and the final versions of the dataframe(s) you built
4. A folder named "images" that contains saved .png versions of your static plots
6. Your writeup in markdown format, named as "README.md"

### Suggestions and Tips
 * I encourage you to create a project on a subject that is relevant to your interests, and other Harris classes.  If your research idea is a much larger project, think of how you can develop a basic framework for it using this project, which can then later be expanded into a proper research project.
 * The exact components of your project are negotiable; use the five steps above as a starting point, and contact me for discussion and approval if you wish to do something differently.
 * If you feel stuck coming up with research ideas, feel free to contact me or one of the TAs so we can discuss your interests and make suggestions.
 * When you have a research idea, please send me a short, informal proposal describing the idea so we can make sure you're on the right track before you begin work.
 * You may use libraries and methods we did not go over in class, but ones that we did go over should be preferred if they duplicate the functionality.  Remember all citation rules from the academic dishonesty policy in the syllabus.
 * Effort put into organizing your code and making it readable, by, for example, following the PEP8 style guide, good usage of functions, and appropriate comments will be rewarded.
 * Similarly, your GitHub repo should be organized - do not leave useless files there (e.g. DS_store files), and keep things in folders
 * The entire point of reproducible research is to make it possible for others (and for a future you who has had time to forget what you did and why) to understand, replicate, and modify your work.  Keeping this in mind as you work will be good for your grade, and helpful to you in the future if you expand on the project.
 * You are welcome to experiment with free shinyapps.io pages, but it is not required.  Shiny will run slowly on free clusters, paricularly if your data is large.  Additionally, Shiny for Python is still in development, and in my experience uploading it to shinyapps.io is not fully functional yet.
