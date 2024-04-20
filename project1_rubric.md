# Rubric for Project Proposal

Your project proposal should be written in markdown format.  It will be saved in the same directory as this rubric, and simply named "proposal.md."  It should not be longer than 3 pages max, and may be shorter.  It will have the following sections.

### Title

A nice short title for your project, helping me to understand roughly what it is you are trying to do.

### Team

Each project will have a team of 3-5 people, an one person will serve as the point of contact for the team who will "own" the repository.  Everyone will be granted access to this repository.  This section should include the full names of each team member, **along with their github ids**, and indicate the point of contact for the group.

### Introduction

This section will introduce your project.  It will _briefly_ answer the following questions from [Heilmeier's catechism](https://www.darpa.mil/work-with-us/heilmeier-catechism).

- What are you trying to do? Articulate your objectives using absolutely no jargon.
- What is new in your approach and why do you think it will be successful?  (be brief here, you can expand more later!)
- Who cares? If you are successful, what difference will it make?

### Literature Review

This section will answer the second question from Heilmeier's catechism:

- How is it done today, and what are the limits of current practice?

Here, you will review methods that have been attempted (use [Google Scholar](https://scholar.google.com) to find out), or cite literature to provide evidence that your method is novel.  You should include citations to reviewed material.  Use [github's markdown syntax for footnotes](https://docs.github.com/en/get-started/writing-on-github/getting-started-with-writing-and-formatting-on-github/basic-writing-and-formatting-syntax#footnotes)[^1].

Also use this section to establish _stakeholder needs_.  That is, for stakeholders that have specific but perhaps non-obvious needs (e.g., a city traffic planner will need to know about temporal variation in pedestrian traffic and the density of office buildings in order to determine the best placement of traffic signals).  You should list each of your stakeholders here, explain why they are stakeholders, and the clarify needs. Recall from our lecture that I do not expect you to do your own stakeholder interviews, but I do expect you to do a little research online.

### Data and Methods

This section will introduce your data and specify your modeling approach.

#### Data

##### If you have data:

You will include a link to your data, along with summary information (e.g., how many columns, rows, and the types of features).  Also include some discussion about the data's provenance - how do you know the data is reliable?  Does it have meta data, and if not, what else do you know about it?  If you anticipate needing more than one dataset, please indicate each dataset you anticipate needing and provide evidence that those datasets are available.

##### If you don't have data:

You will include what you have done to find data potential and how you will evaluate sources of data.

#### Methods

Describe your modeling approach.  What sorts of transformations / preprocessing are going to be necessary?  What sorts of modeling techniques will you apply? How are you going to evaluate your models (note that your evaluation should be consistent with stakeholder needs)?

### Project Plan

Tell me what you are going to do and when.  We have roughly two months (final report due during the first week of May).  What milestones do you hope to achieve?  You might organize this as a table.  For example:

Period|Activity|Milestone
|---|---|---|
|3/4 - 3/11|Stakeholder analysis </br> EDA </br> Initial exploration with LSTM networks on toy data|Completed stakeholder analysis and data exploration. Additional datasets identified as necessary.
|3/11 - 3/18|Preprocessing and modeling refinements. Continuing exploration of LSTM and other temporal ML techniques| Initial pass with cleaned data. Candidate approaches for modeling finalized.

etc.

### Risks

This section will describe potential risks to the project. This should include both potential pitfalls and roadblocks, as well as any approaches to mitigation.  What will you do if some aspect of your plan fails?

# Grading

- Does your proposal include all of the above mentioned sections? [10 points]
- Have you clearly identifed your stakeholders and their needs [10 points]
- Have you answered the 5 Heilmeier questions mentioned effectively [20 points]
- Is you proposal plan feasible, and do you have sufficient detail to give me confidence that you will succeed? [10 points] 
- Is your proposal well written and devoid of 'weasel words': https://docbarraiser.com/what-the-heck-are-weasel-words/ [10 points]

[^1]: Like this
