import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
from api_helper.api_models import PipelineModel
from api_helper.pipeline import Pipeline
from api_helper.pipeline_utils import get_loaders_with_params, get_transformers_with_params, get_exporters_with_params,\
    create_pipeline_object
from loaders.csv_loader import CSVLoader

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

pipelines = []


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


@app.get('/get_loaders_params')
async def get_loaders_params():
    return JSONResponse(status_code=200, content=get_loaders_with_params())


@app.get('/get_transformers_params')
async def get_transformers_params():
    return JSONResponse(status_code=200, content=get_transformers_with_params())


@app.get('/get_exporters_params')
async def get_exporters_params():
    return JSONResponse(status_code=200, content=get_exporters_with_params())


@app.post('/create_pipeline')
async def create_pipeline(pipeline: PipelineModel):

    for step in pipeline.steps:
        if 'name' not in step.keys() or 'params' not in step.keys():
            return JSONResponse(status_code=400, content="Please provide the Name and Params for every step!")

    steps = []
    errors = 0
    for step in pipeline.steps:
        try:
            obj = create_pipeline_object(step)
            steps.append((step['name'], obj))
        except ImportError:
            errors += 1

    if errors > 0:
        return JSONResponse(status_code=500, content="Could not create all the steps that were provided!")

    pipelines.append(Pipeline(steps))
    return JSONResponse(status_code=200, content="Pipeline created successfully!")

if __name__ == '__main__':
    # uvicorn.run(app, host='0.0.0.0', port=8000)
    print(create_pipeline_object({
        "csv_loader": {
            "path": "dataspace/iris_dataset"
        }
    }))
