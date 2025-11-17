from fastapi import FastAPI, HTTPException, Depends
from . import account_api_wrap as apw
from . import account_api_valid as apv
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

@app.get("/neuronXY/users")
def get_user_api(current_user: str = Depends(get_current_user)):
    try:
        return apw.get_user(current_user)

    except Exception as e:
        print(f"Error creating user: {e}")
        raise HTTPException(status_code=500, detail="Failed to create user")

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