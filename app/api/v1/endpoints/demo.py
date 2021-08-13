from fastapi import APIRouter

router = APIRouter()


@router.get(
    "/demo", tags=["demo"], response_model=dict,
)
async def demo():
    return {"Hello": "World"}
