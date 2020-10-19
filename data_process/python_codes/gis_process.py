def calculate_total_travel_distances(df):
    from geopy.distance import great_circle

    lastClusterNumber = df['cluster'].idxmax()
    # df['distances'] = -1
    total_distance = 0;
    lastIndex = -1
    for index, point in df.iterrows():
        if lastIndex == -1:
            # df.set_value(index, 'distances', 0)
            total_distance = 0;
            lastIndex = index
        else:
            distance = great_circle(
                (df.loc[lastIndex]['latitude'], df.loc[lastIndex]['longitude']),
                (df.loc[index]['latitude'], df.loc[index]['longitude'])).meters
            total_distance += distance
            # df.set_value(index, 'distances', distance + df.loc[lastIndex]['distances'])
            lastIndex = index

    return total_distance




def GISProcess(df):
    import math
    import pyproj
    import json
    import numpy as np
    from shapely.geometry import Point
    from shapely.geometry import Polygon

    points = [df['latitude'].values, df['longitude'].values]
    w = np.matrix(points)
    t = np.squeeze(np.asarray(w.T))


    ##projection transform
    EPSG3395 = pyproj.Proj("+init=EPSG:3395")
    EPSG4326 = pyproj.Proj("+init=EPSG:4326")

    t_points = []
    X = []
    Y = []
    for ix,iy in np.ndindex(t.shape):
        x, y = pyproj.transform(EPSG4326,EPSG3395,  t[ix,1],t[ix,0])
        X.append(x)
        Y.append(y)
        t_points.append(Point(x,y))


    coords = [(p.x, p.y) for p in t_points]
    geom = Polygon(coords)

    # df['area'] = int(math.ceil(geom.convex_hull.area/1000.0/1000.0))
    area = int(round(geom.convex_hull.area/1000.0/1000.0))
    ellipsePolygonResult = standard_deviation_ellipse(X, Y)
    SDELength, SDEArea, CenterX, CenterY, SDEx, SDEy, degreeRotation, AngleRotation1, AngleRotation2 = ellipsePolygonResult

    jsonResult = {'CenterX':CenterX,  'CenterY':CenterY, 'SDEx': SDEx, 'SDEy':SDEy, 'AngleRotation':AngleRotation2, 'SDELength':SDELength, 'SDEArea':SDEArea,  'MCPArea':area}
    return jsonResult


def standard_deviation_ellipse(x, y):

    import numpy as np
    weight = 1.0 #here

    meanx = np.mean(x)  # or  np.sum(x)/n if weight
    meany = np.mean(y)  #  np.sum(y)/n weight
    n = len(x)
    xd = x-meanx
    yd = y-meany
    xyw = sum(xd*yd * weight)
    x2w = sum(xd**2.0 * weight)
    y2w = sum(yd**2.0 * weight)


    # diffXY
    top1 = x2w - y2w

    # sum1
    top2 = np.sqrt( (x2w - y2w)**2.0 + 4.0 * (xyw)**2.0)

    # denom
    bottom = (2.0 * xyw)

    if not abs(bottom) > 0:
        arcTanValue = 0.0
    else:
        temp = (top1 + top2) / bottom
        arcTanValue = np.arctan(temp)

    if arcTanValue < 0.0:
        arcTanValue += (np.pi / 2.0)

    costheta = np.cos(arcTanValue)
    sintheta = np.sin(arcTanValue)

    sin2theta = sintheta**2.0
    cos2theta = costheta**2.0
    sinthetacostheta =  sintheta * costheta
    SDEx = np.sqrt(2.0) * np.sqrt( ((x2w)*(cos2theta) - 2.0*(xyw)*(sinthetacostheta) + (y2w)*(sin2theta))  / (n ))
    SDEy = np.sqrt(2.0) * np.sqrt( ((x2w)*(sin2theta) + 2.0*(xyw)*(sinthetacostheta) + (y2w)*(cos2theta)) / (n ))

    degreeRotation = 360.0 - (arcTanValue * 57.2957795)

    AngleRotation1 = np.pi / 180.0 * degreeRotation

    AngleRotation2 = 360.0 - degreeRotation
    if SDEx > SDEy:
        AngleRotation2 += 90.0
        if AngleRotation2 > 360.0:
            AngleRotation2 = AngleRotation2 - 180.0


    # print( SDEx, SDEy, degreeRotation,  AngleRotation1, AngleRotation2)

    SEDResult = (meanx, meany, SDEx, SDEy, degreeRotation, AngleRotation1, AngleRotation2)
    polygonResult = ellipse_polygon(SEDResult)

    return polygonResult

def ellipse_polygon (SEDResult):
    import numpy as np
    from shapely.geometry import Point
    from shapely.geometry import Polygon

    CenterX, CenterY, SDEx, SDEy, degreeRotation, AngleRotation1, AngleRotation2 = SEDResult

    cosAngle = np.cos(AngleRotation1)
    sinAngle = np.sin(AngleRotation1)
    SDEx2 = SDEx**2.0
    SDEy2 = SDEy**2.0

    poly = []

    for degreeRatate in np.arange(0, 360):

            angle = np.pi / 180.0 * degreeRatate
            tanV2 = np.tan(angle)**2.0
            dX = np.sqrt((SDEx2 * SDEy2) / (SDEy2 + (SDEx2 * tanV2)))
            dY = np.sqrt((SDEy2 * (SDEx2 - dX**2.0)) / SDEx2)

            if 90 <= degreeRatate < 180:
                dX = -dX
            elif 180 <= degreeRatate < 270:
                dX = -dX
                dY = -dY
            elif degreeRatate >= 270:
                dY = -dY

            dXr = dX * cosAngle - dY * sinAngle
            dYr = dX * sinAngle + dY * cosAngle

            pntX = dXr + CenterX
            pntY = dYr + CenterY
            pnt = Point(pntX, pntY)
            poly.append(pnt)

    coords = [(p.x, p.y) for p in poly]
    geom = Polygon(coords)
    polygonResult = (geom.length, geom.area, CenterX, CenterY, SDEx, SDEy, degreeRotation, AngleRotation1, AngleRotation2)
    # print(geom.length, geom.area)
    return polygonResult


def calculate_total_location_distances(df):
    import numpy as np
    from geopy.distance import great_circle
    import pandas as pd

    clusteredRecords = df.loc[df['cluster'] > 0]
    maxClusterNo = df['cluster'].max()
    locationCentre = []
    locationInfo = pd.DataFrame(columns = ['start', 'end', 'longitude', 'latitude','cluster'])
    for index in range(1, maxClusterNo+1):
        temp =  clusteredRecords.loc[clusteredRecords['cluster'] == index]
        length = len(temp)
        cluster = temp.iloc[0]['cluster']
        start = temp.iloc[0]['date_time']
        end = temp.iloc[-1]['date_time']
        meanlong = np.mean(temp['longitude'].astype(np.float))
        meanlat = np.mean(temp['latitude'].astype(np.float))
        center = [meanlat, meanlong]
        locationCentre.append(center)
        s = pd.Series({'start': start, 'end':end, 'longitude':meanlong, 'latitude':meanlat, 'cluster': cluster})
        locationInfo = locationInfo.append(s, ignore_index=True)
        # print (temp)
        # print (locationInfo)


    # print(locationCentre)
    total_distance = 0;
    lastIndex = -1
    for index in range(len(locationCentre)):
        # print(index)
        # print(locationCentre[index])
        # print(locationCentre[index][0])
        # print(locationCentre[index][1])
        if lastIndex == -1:
            # df.set_value(index, 'distances', 0)
            total_distance = 0;
            lastIndex = index
        else:
            distance = great_circle(
                (locationCentre[lastIndex][0], locationCentre[lastIndex][1]),
                (locationCentre[index][0], locationCentre[index][1])).meters
            total_distance += distance
            # df.set_value(index, 'distances', distance + df.loc[lastIndex]['distances'])
            lastIndex = index

    tldResult = (locationInfo, total_distance)
    return tldResult