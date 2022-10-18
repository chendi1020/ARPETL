# ETL for ARP Dashboard 2.0
The program is used to take the data stored in the research template format, clean and transform it into datasets that supports building of ARP dasbhoard 2.0

## how to run the program
### prerequisite :
- python (3.8) installed 
- <code> pip install pandas </code>
- <code> pip install numpy </code>

### steps:
- have your terminal in the ETL directory
- check the input and output data path specified in the config.yml. If you have the research data file located in a different directory, or if you want to push the output data into a different directory, you need to change the path values in the config.yml (line 2 and line 166)
- type the command <code> python run.py </code>