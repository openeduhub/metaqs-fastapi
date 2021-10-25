from fastapi import APIRouter

router = APIRouter()


@router.get(
    "/demo", tags=["Demo"], response_model=dict,
)
async def demo():
    return {"Hello": "OpenEduHub"}
