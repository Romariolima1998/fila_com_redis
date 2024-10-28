from fastapi import FastAPI
from redis import Redis
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

app = FastAPI()

# Configurações do Redis
redis_client = Redis(host='localhost', port=6379, db=0)

# Configurações do SQLAlchemy
DATABASE_URL = "sqlite:///database.db"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

class Task(Base):
    __tablename__ = 'tasks'
    id = Column(Integer, primary_key=True, index=True)
    description = Column(String, index=True)

Base.metadata.create_all(engine)

def add_task_to_queue(task_description: str):
    task_id = redis_client.incr('task_id')
    redis_client.hset(f'task:{task_id}', mapping={'id': task_id, 'description': task_description})
    redis_client.rpush('task_queue', task_id)

def process_task_from_queue():
    task_id = redis_client.lpop('task_queue')
    if task_id:
        task_data = redis_client.hgetall(f'task:{task_id.decode("utf-8")}')
        if task_data:
            task = Task(id=int(task_data[b'id']), description=task_data[b'description'].decode("utf-8"))
            session.add(task)
            session.commit()
            redis_client.delete(f'task:{task_id.decode("utf-8")}')
            print(f"Processed task: {task.description}")
        else:
            print("Task not found")
    else:
        print("No tasks in the queue")

@app.post("/tasks/")
def create_task(description: str):
    add_task_to_queue(description)
    return {"message": "Task added to the queue"}

@app.post("/process_tasks/")
def process_tasks():
    process_task_from_queue()
    return {"message": "Task processed"}

