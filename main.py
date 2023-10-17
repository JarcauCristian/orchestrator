import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse

from loaders.csv_loader import CSVLoader

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get('/')
async def root():
    return JSONResponse(status_code=200, content="Hello From the Pipeline API!")


@app.get('/get_statistics')
async def get_statistics(dataset_path: str, req: Request):
    token = req.headers.get('Authorization')
    print(token)

    if token is None:
        return JSONResponse(status_code=401, content="You don't have access to this component")

    csv_loader = CSVLoader(path=dataset_path)
    csv_loader.execute(token.split(" ")[0])

    return JSONResponse(status_code=200, content=csv_loader.get_statistics())


@app.post('/create_pipeline')
async def create_pipeline():
    pass


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
