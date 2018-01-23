#from numpy import genfromtxt
from time import time
from datetime import datetime
from sqlalchemy import Column, Integer, Float, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def Load_Data(file_name):
    data = genfromtxt(file_name, delimiter=',', skip_header=1, converters={0: lambda s: str(s)})
    return data.tolist()

Base = declarative_base()

class North_Objects(Base):
    #Tell SQLAlchemy what the table name is and if there's any table-specific arguments it should know about
    __tablename__ = 'North_Objects'
    __table_args__ = {'sqlite_autoincrement': True}
    #tell SQLAlchemy the name of column and its attributes:
    id = Column(Integer, primary_key=True, nullable=False) 
    date = Column(Date)
    name = Column(Float)
    dimensions = Column(Float)
    mount = Column(Float)
    misc = Column(Float)
    

if __name__ == "__main__":
    t = time()

    #Create the database
    engine = create_engine('sqlite:///csv_test.db')
    Base.metadata.create_all(engine)

    #Create the session
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()

    try:
        file_name = "4E.csv" #sample CSV file used:  http://www.google.com/finance/historical?q=NYSE%3AT&ei=W4ikVam8LYWjmAGjhoHACw&output=csv
        data = Load_Data(file_name) 

        for i in data:
            record = North_Objects(**{
                'case' : i[0],
                'date' : datetime.strptime(i[1], '%d-%b-%y').date(),
                'name' : i[2],
                'dimensions' : i[3],
                'mount' : i[4],
                'misc' : i[5]
            })
            s.add(record) #Add all the records

        s.commit() #Attempt to commit all the records
    except:
        s.rollback() #Rollback the changes on error
    finally:
        s.close() #Close the connection
    print ("Time elapsed: " + str(time() - t) + " s.") #0.091s