import pymongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["local_mongo"]
mycol = mydb['charts_by_count']

db_chart = {
      "data": [
        {
          "actual": "Number Tile",
          "alias": "numberTile",
          "keywords": "total, net, average, maximum, minimum, max, min, highest, lowest",
          "noOfCategoryDimensions": 0,
          "noOfGoalMeasure": 1,
          "noOfMeasure": 0,
          "noOfTimelineDimensions": 0
        },
        {
          "actual": "Number Tile",
          "alias": "numberTile",
          "keywords": "total, net, average, maximum, minimum, max, min, highest, lowest",
          "noOfCategoryDimensions": 0,
          "noOfGoalMeasure": 1,
          "noOfMeasure": 0,
          "noOfTimelineDimensions": 1
        },
        {
          "actual": "Number Tile",
          "alias": "numberTile",
          "keywords": "total, net, average, maximum, minimum, max, min, highest, lowest",
          "noOfCategoryDimensions": 1,
          "noOfGoalMeasure": 1,
          "noOfMeasure": 0,
          "noOfTimelineDimensions": 1
        },
        {
          "actual": "Single Column Vertical Bar chart",
          "alias": "barChart",
          "keywords": "compare, by, -wise",
          "noOfCategoryDimensions": 1,
          "noOfGoalMeasure": 1,
          "noOfMeasure": 0,
          "noOfTimelineDimensions": 0
        },
        {
          "actual": "2 Column Vertical Bar chart",
          "alias": "barChart",
          "keywords": "compare, vs, by, -wise, and",
          "noOfCategoryDimensions": 1,
          "noOfGoalMeasure": 2,
          "noOfMeasure": 0,
          "noOfTimelineDimensions": 0
        },
        {
          "actual": "Number Tile",
          "alias": "numberTile",
          "keywords": "total",
          "noOfCategoryDimensions": 0,
          "noOfMeasure": 1
        },
        {
          "actual": "Number Tile",
          "alias": "numberTile",
          "keywords": "total, net, average, maximum, minimum, max, min, highest, lowest",
          "noOfCategoryDimensions": 0,
          "noOfGoalMeasure": 0,
          "noOfMeasure": 1,
          "noOfTimelineDimensions": 0
        },
        {
          "actual": "Single Line chart",
          "alias": "lineChart",
          "keywords": "trend, by,-wise,-ly",
          "noOfCategoryDimensions": 0,
          "noOfGoalMeasure": 0,
          "noOfMeasure": 1,
          "noOfTimelineDimensions": 1
        },
        {
          "actual": "Pie chart",
          "alias": "pieChart",
          "keywords": "comprise, of, split, share, by, composed, consist, percent, %",
          "noOfCategoryDimensions": 0,
          "noOfGoalMeasure": 0,
          "noOfMeasure": 1,
          "noOfTimelineDimensions": 1
        },
        {
          "actual": "Number Tile",
          "alias": "numberTile",
          "keywords": "total, net, average, maximum, minimum, max, min, highest, lowest",
          "noOfCategoryDimensions": 0,
          "noOfGoalMeasure": 0,
          "noOfMeasure": 1,
          "noOfTimelineDimensions": 1
        },
        {
          "actual": "Absolute stacked bar chart",
          "alias": "stackedBarChart",
          "keywords": "split, trend, by,-wise,-ly, and, &",
          "noOfCategoryDimensions": 1,
          "noOfGoalMeasure": 0,
          "noOfMeasure": 1,
          "noOfTimelineDimensions": 1
        },
        {
          "actual": "100% stacked bar chart",
          "alias": "stackedBarChart",
          "keywords": "trend, by,-wise,-ly, and, &, percent, %, comprise, of, share, by, composed, consist, relative",
          "noOfCategoryDimensions": 1,
          "noOfGoalMeasure": 0,
          "noOfMeasure": 1,
          "noOfTimelineDimensions": 1
        },
        {
          "actual": "Waterflow chart",
          "alias": "waterflowChart",
          "keywords": "walk, change, by, from, over, to, cumulate",
          "noOfCategoryDimensions": 1,
          "noOfGoalMeasure": 0,
          "noOfMeasure": 1,
          "noOfTimelineDimensions": 1
        },
        {
          "actual": "Number Tile",
          "alias": "numberTile",
          "keywords": "total, net, average, maximum, minimum, max, min, highest, lowest",
          "noOfCategoryDimensions": 1,
          "noOfGoalMeasure": 0,
          "noOfMeasure": 1,
          "noOfTimelineDimensions": 1
        },
        {
          "actual": "Absolute stacked bar chart",
          "alias": "stackedBarChart",
          "keywords": "split, compare, vs, by, -wise, and",
          "noOfCategoryDimensions": 2,
          "noOfGoalMeasure": 0,
          "noOfMeasure": 1,
          "noOfTimelineDimensions": 0
        },
        {
          "actual": "100% stacked bar chart",
          "alias": "stackedBarChart",
          "keywords": "percent, %, comprise, of, share, by, composed, consist, relative",
          "noOfCategoryDimensions": 2,
          "noOfGoalMeasure": 0,
          "noOfMeasure": 1,
          "noOfTimelineDimensions": 0
        },
        {
          "actual": "KPI Tile",
          "alias": "kpiTile",
          "keywords": "compare, vs, for, and, &",
          "noOfCategoryDimensions": 0,
          "noOfGoalMeasure": 1,
          "noOfMeasure": 1,
          "noOfTimelineDimensions": 1
        },
        {
          "actual": "2 Column Vertical Bar chart",
          "alias": "barChart",
          "keywords": "compare, vs, by, and, &",
          "noOfCategoryDimensions": 0,
          "noOfGoalMeasure": 1,
          "noOfMeasure": 1,
          "noOfTimelineDimensions": 1
        },
        {
          "actual": "2 Column Vertical Bar chart",
          "alias": "barChart",
          "keywords": "compare, vs, by, -wise, and",
          "noOfCategoryDimensions": 1,
          "noOfGoalMeasure": 1,
          "noOfMeasure": 1,
          "noOfTimelineDimensions": 0
        },
        {
          "actual": "Pie chart",
          "alias": "pieChart",
          "keywords": "comprise, of, split, share, total, by, composed, consist, percent, %",
          "noOfCategoryDimensions": 2,
          "noOfMeasure": 1
        },
        {
          "actual": "Absolute stacked bar chart",
          "alias": "stackedBarChart",
          "keywords": "split, trend, by,-wise,-ly, and, &",
          "noOfCategoryDimensions": 2,
          "noOfMeasure": 1
        },
        {
          "actual": "100% stacked bar chart",
          "alias": "stackedBarChart",
          "keywords": "trend, by,-wise,-ly, and, &, percent, %, comprise, of, share, total, by, composed, consist, relative",
          "noOfCategoryDimensions": 2,
          "noOfMeasure": 1
        },
        {
          "actual": "Absolute stacked bar chart",
          "alias": "stackedBarChart",
          "keywords": "split, compare, vs, by, -wise, and",
          "noOfCategoryDimensions": 2,
          "noOfMeasure": 1
        },
        {
          "actual": "100% stacked bar chart",
          "alias": "stackedBarChart",
          "keywords": "percent, %, comprise, of, share, total, by, composed, consist, relative",
          "noOfCategoryDimensions": 2,
          "noOfMeasure": 1
        },
        {
          "actual": "Waterflow chart",
          "alias": "waterflowChart",
          "keywords": "walk, change, by, from, over, to, cumulate",
          "noOfCategoryDimensions": 2,
          "noOfMeasure": 1
        },
        {
          "actual": "Number Tile",
          "alias": "numberTile",
          "keywords": "for, total",
          "noOfCategoryDimensions": 2,
          "noOfMeasure": 1
        },
        {
          "actual": "Single Column Vertical Bar chart",
          "alias": "barChart",
          "keywords": "compare, by, -wise",
          "noOfCategoryDimensions": 1,
          "noOfGoalMeasure": 0,
          "noOfMeasure": 1,
          "noOfTimelineDimensions": 0
        },
        {
          "actual": "Number Tile",
          "alias": "numberTile",
          "keywords": "total, net, average, maximum, minimum, max, min, highest, lowest",
          "noOfCategoryDimensions": 1,
          "noOfGoalMeasure": 0,
          "noOfMeasure": 1,
          "noOfTimelineDimensions": 0
        },
        {
          "actual": "Pie chart",
          "alias": "pieChart",
          "keywords": "comprise, of, split, share, by, composed, consist, percent, %",
          "noOfCategoryDimensions": 1,
          "noOfGoalMeasure": 0,
          "noOfMeasure": 1,
          "noOfTimelineDimensions": 0
        },
        {
          "actual": "Single Column Vertical Bar chart",
          "alias": "barChart",
          "keywords": "compare, by, -wise",
          "noOfCategoryDimensions": 1,
          "noOfMeasure": 1
        },
        {
          "actual": "Number Tile",
          "alias": "numberTile",
          "keywords": "for, total",
          "noOfCategoryDimensions": 1,
          "noOfMeasure": 1
        },
        {
          "actual": "Single Column Vertical Bar chart",
          "alias": "barChart",
          "keywords": "compare, by, -wise",
          "noOfCategoryDimensions": 1,
          "noOfMeasure": 1
        },
        {
          "actual": "Single Line chart",
          "alias": "lineChart",
          "keywords": "trend, by,-wise,-ly",
          "noOfCategoryDimensions": 1,
          "noOfMeasure": 1
        },
        {
          "actual": "Pie chart",
          "alias": "pieChart",
          "keywords": "comprise, of, split, share, total, by, composed, consist, percent, %",
          "noOfCategoryDimensions": 1,
          "noOfMeasure": 1
        },
        {
          "actual": "Two Line chart",
          "alias": "multiLineChart",
          "keywords": "trend, by,-wise,-ly, and, &, compare",
          "noOfCategoryDimensions": 0,
          "noOfGoalMeasure": 0,
          "noOfMeasure": 2,
          "noOfTimelineDimensions": 1
        },
        {
          "actual": "2 Column Vertical Bar chart",
          "alias": "barChart",
          "keywords": "compare, vs, by, -wise, and",
          "noOfCategoryDimensions": 1,
          "noOfGoalMeasure": 0,
          "noOfMeasure": 2,
          "noOfTimelineDimensions": 0
        },
        {
          "actual": "Scatter chart",
          "alias": "scatterChart",
          "keywords": "relate, correlation, pattern, between",
          "noOfCategoryDimensions": 1,
          "noOfGoalMeasure": 0,
          "noOfMeasure": 2,
          "noOfTimelineDimensions": 0
        },
        {
          "actual": "2 column absolute stacked bar chart",
          "alias": "stackedBarChart",
          "keywords": "trend, by,-wise,-ly, and, &, vs",
          "noOfCategoryDimensions": 1,
          "noOfGoalMeasure": 0,
          "noOfMeasure": 2,
          "noOfTimelineDimensions": 1
        },
        {
          "actual": "2 Column Vertical Bar chart",
          "alias": "barChart",
          "keywords": "compare, vs, by, -wise, and",
          "noOfCategoryDimensions": 1,
          "noOfMeasure": 2
        },
        {
          "actual": "KPI Tile",
          "alias": "kpiTile",
          "keywords": "compare, vs, for, and, &",
          "noOfCategoryDimensions": 1,
          "noOfMeasure": 2
        },
        {
          "actual": "2 Column Vertical Bar chart",
          "alias": "barChart",
          "keywords": "compare, vs, by, and, &",
          "noOfCategoryDimensions": 1,
          "noOfMeasure": 2
        },
        {
          "actual": "Two Line chart",
          "alias": "multiLineChart",
          "keywords": "trend, by,-wise,-ly, and, &",
          "noOfCategoryDimensions": 1,
          "noOfMeasure": 2
        },
        {
          "actual": "Scatter chart",
          "alias": "scatterChart",
          "keywords": "relate, correlation, pattern, between",
          "noOfCategoryDimensions": 1,
          "noOfMeasure": 2
        },
        {
          "actual": "3 Column Vertical Bar chart",
          "alias": "barChart",
          "keywords": "compare, vs, by, -wise, and",
          "noOfCategoryDimensions": 1,
          "noOfGoalMeasure": 0,
          "noOfMeasure": 3,
          "noOfTimelineDimensions": 0
        },
        {
          "actual": "Bubble chart",
          "alias": "bubbleChart",
          "keywords": "relate, correlation, pattern, between, among",
          "noOfCategoryDimensions": 1,
          "noOfGoalMeasure": 0,
          "noOfMeasure": 3,
          "noOfTimelineDimensions": 0
        },
        {
          "actual": "3 Line chart",
          "alias": "multiLineChart",
          "keywords": "trend, by,-wise,-ly, and, &, compare",
          "noOfCategoryDimensions": 0,
          "noOfGoalMeasure": 0,
          "noOfMeasure": 3,
          "noOfTimelineDimensions": 1
        },
        {
          "actual": "3 Column Vertical Bar chart",
          "alias": "barChart",
          "keywords": "compare, vs, by, -wise, and",
          "noOfCategoryDimensions": 1,
          "noOfMeasure": 3
        },
        {
          "actual": "3 Line chart",
          "alias": "multiLineChart",
          "keywords": "trend, by,-wise,-ly, and, &",
          "noOfCategoryDimensions": 1,
          "noOfMeasure": 3
        },
        {
          "actual": "Bubble chart",
          "alias": "bubbleChart",
          "keywords": "relate, correlation, pattern, between, among",
          "noOfCategoryDimensions": 1,
          "noOfMeasure": 3
        },
        {
          "actual": "4 Line chart",
          "alias": "multiLineChart",
          "keywords": "trend, by,-wise,-ly, and, &",
          "noOfCategoryDimensions": 1,
          "noOfMeasure": 4
        },
        {
          "actual": "4 Line chart",
          "alias": "multiLineChart",
          "keywords": "trend, by,-wise,-ly, and, &, compare",
          "noOfCategoryDimensions": 0,
          "noOfGoalMeasure": 0,
          "noOfMeasure": 4,
          "noOfTimelineDimensions": 1
        }
      ]
    }

charts = db_chart['data']
new_charts = []
i =1
for cc in charts:
    if 'noOfGoalMeasure' not in cc.keys():
        cc['dimension'] = tuple([cc['noOfMeasure'], cc['noOfCategoryDimensions']])
    else:
        cc['dimension'] = tuple([cc['noOfMeasure'], cc['noOfGoalMeasure'], cc['noOfCategoryDimensions'], cc['noOfTimelineDimensions']])
    new_charts.append(cc)
    print(cc['dimension'], " ", i)
    i = i+1
    mycol.insert_one({'data' : cc})

