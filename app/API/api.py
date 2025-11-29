from fastapi import FastAPI, HTTPException, Depends
from API import api_wrap as apw
from API import api_valid as apv
from fastapi.security import OAuth2PasswordBearer
import jwt
from jwt.exceptions import InvalidTokenError, ExpiredSignatureError, DecodeError
from config import API_SECRET_KEY

app = FastAPI()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

async def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, API_SECRET_KEY, algorithms=["HS256"])
        username = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        return username
    except ExpiredSignatureError:
    # Token has expired
        raise HTTPException(status_code=401, detail="Token has expired")
    except DecodeError:
        # Token is malformed or has invalid signature
        raise HTTPException(status_code=401, detail="Invalid token")
    except InvalidTokenError:
        # Catch-all for any other JWT errors
        raise HTTPException(status_code=401, detail="Could not validate credentials")

"""User account related endpoints"""

@app.get("/neuronXY/users")
def get_user_api(current_user: str = Depends(get_current_user)):
    try:
        return apw.get_user(current_user)

    except Exception as e:
        print(f"Error creating user: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch user")

@app.post("/neuronXY/users")
def create_user_api(acct_details: apv.User):
    try:
        user_prepro = acct_details.model_validate(acct_details)
        user_postpro = user_prepro.model_dump()
        result = apw.create_creds(user_postpro)
        return result

    except ValueError as e:
        error_msg = str(e).lower()
        if "already taken" in error_msg:
            raise HTTPException(status_code=409, detail=str(e))
        else:
            raise HTTPException(status_code=400, detail=str(e))
            
    except Exception as e:
        print(f"Error creating user: {e}")
        raise HTTPException(status_code=500, detail="Failed to create user")
    
@app.post('/neuronXY/login')
def user_login(creds: apv.LoginCreds):
    try:
        user_prepro = creds.model_validate(creds)
        user_postpro = user_prepro.model_dump()
        result = apw.user_login(user_postpro)
        return result
    
    except ValueError as e:
        error_msg = str(e).lower()
        if 'WAS NOT VALID' in error_msg: 
            raise HTTPException(status_code=401, detail=str(e))
        else:
            raise HTTPException(status_code=400, detail=str(e))
        
    except Exception as e:
        print(f"Error creating user: {e}")
        raise HTTPException(status_code=500, detail="Failed to log in")
    
@app.delete('/neuronXY/users/{user_id}')
async def delete_user(user_id: int, current_user: str = Depends(get_current_user)):
    try:
        apw.delete_user(current_user, user_id)
    
    except Exception as e:
        print(f"Error deleting user: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete user")
    
@app.put('/neuronXY/login/{user_id}/reset_password')
async def update_password(user_id: int, creds: apv.newPassword, current_user: str = Depends(get_current_user)):
    try:
        user_prepro = creds.model_validate(creds)
        user_postpro = user_prepro.model_dump()
        apw.change_password(dict({'user_id': user_id}, **user_postpro))

    except ValueError as e:
        error_msg = str(e).lower()
        if 'do not match' in error_msg: 
            raise HTTPException(status_code=401, detail=str(e))

    except Exception as e:
        print(f"Error deleting user: {e}")
        raise HTTPException(status_code=500, detail="Failed to update the user's password")
    
"""Cortex node level related endpoints"""

@app.get("/neuronXY/users/{user_id}/cortex/nodes")
def get_nodes_api(user_id: int, current_user: str = Depends(get_current_user)):
    try:
        return apw.list_nodes(user_id, current_user)

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    except Exception as e:
        print(f"Error creating user: {e}")
        raise HTTPException(status_code=500, detail="Failed to list nodes for user")

@app.get("/neuronXY/users/{user_id}/cortex/node")
def get_node_api(user_id: int, node_name: apv.cortexNode, current_user: str = Depends(get_current_user)):
    try:
        nn_prepro = node_name.model_validate(node_name)
        nn_postpro = nn_prepro.model_dump()
        return apw.get_node(current_user, user_id, nn_postpro)

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    except Exception as e:
        print(f"Error creating user: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch cortex node")
    
@app.post("/neuronXY/users/{user_id}/cortex/node")
def create_node_api(user_id: int, node_name: apv.cortexNode, current_user: str = Depends(get_current_user)):
    try:
        nn_prepro = node_name.model_validate(node_name)
        nn_postpro = nn_prepro.model_dump()
        apw.mk_node(current_user, user_id, nn_postpro)

    except ValueError as e:
        error_msg = str(e).lower()

        if 'node name is already in use' in error_msg:
            raise HTTPException(status_code=409, detail=str(e))
        else:
            raise HTTPException(status_code=404, detail=str(e))

    except Exception as e:
        print(f"Error creating user: {e}")
        raise HTTPException(status_code=500, detail="Failed to create cortex node")
    
@app.put("/neuronXY/users/{user_id}/cortex/node/{node_id}")
def update_node_api(user_id: int, node_id: int, updates: apv.updateNodeProperties, current_user: str = Depends(get_current_user)):
    try:
        nn_prepro = updates.model_validate(updates)
        nn_postpro = nn_prepro.model_dump()
        apw.update_node(user_id, current_user, node_id, nn_postpro)

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to update node")
    
@app.delete("/neuronXY/users/{user_id}/cortex/node/{node_id}")
def delete_node_api(user_id: int, node_id: int, current_user: str = Depends(get_current_user)):
    try:
        apw.delete_node(user_id, current_user, node_id)

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to update node")