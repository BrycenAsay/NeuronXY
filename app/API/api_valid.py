from pydantic import BaseModel, field_validator, model_validator
from typing import Optional

class User(BaseModel):
    password: str
    username: str

    @field_validator('username')
    def is_valid_username(cls, value):
        if len(value) > 32:
            raise ValueError('Username is too long')
        elif len(value) < 8:
            raise ValueError('Username is too short')
        return value
        
    @field_validator('password')
    def is_valid_password(cls, value):
        if len(value) < 16:
            raise ValueError('Password must be at least 16 characters long!')
        elif len(value) > 50:
            raise ValueError('You\'re supposed to be creating a password not writing an essay :D')
        elif len(list(set([x for x in value]))) < 5:
            raise ValueError('You might want to add more than 4 unique characters to your password lil bro')
        return value
        
    @model_validator(mode='after')
    def check_password_username(self):
        if self.username.lower() in self.password.lower():
            raise ValueError('Do not put your username in your password bro security breaches exist because of sloppy people like you :(')
        return self
    
class LoginCreds(BaseModel):
    username: str
    password: str
    salt: str

class newPassword(BaseModel):
    username: str
    old_password: str
    password: str

    @field_validator('password')
    def is_valid_password(cls, value):
        if len(value) < 16:
            raise ValueError('Password must be at least 16 characters long!')
        elif len(value) > 50:
            raise ValueError('You\'re supposed to be creating a password not writing an essay :D')
        elif len(list(set([x for x in value]))) < 5:
            raise ValueError('You might want to add more than 4 unique characters to your password lil bro')
        return value
    
    @model_validator(mode='after')
    def check_password_username(self):
        if self.username.lower() in self.password.lower():
            raise ValueError('Do not put your username in your password bro security breaches exist because of sloppy people like you :(')
        if self.password.lower() in self.old_password.lower():
            raise ValueError('Your new password cannot contain or be your old password (that\'s why they call it a "new" password :D)')
        return self
    
class cortexNode(BaseModel):
    name: str
    tags: Optional[list[str]] = None

    @field_validator('name')
    def is_valid_node_name(cls, value):
        if (len(value) < 3 or len(value) > 63):
            raise ValueError(f'Node name must be between three and sixty three characters long! "{value}" is a goofy ahh name for a node anyway')
        return value
    
class updateNodeProperties(BaseModel):
    tags: Optional[list[str]] = None