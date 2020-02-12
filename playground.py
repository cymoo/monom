from monorm import *
from pprint import pp


class User(Model):
    _id: ObjectId = ObjectId
    name: str
    email: str
    password: str
    avatar_url: str
    gender: str
    about_me: str
    created_on: datetime = datetime.utcnow
    last_seen: datetime = datetime.utcnow
    disabled: bool = False

    class Meta:
        required = ['name', 'email', 'password']
        indexes = [
            {'key': 'name', 'unique': True}
        ]

    @property
    def main_info(self) -> 'UserInfo':
        return UserInfo(id=self._id, name=self.name, avatar_url=self.avatar_url)

    def __str__(self):
        return '<User: {}>'.format(self.name)


class Comment(EmbeddedModel):
    commenter_id: ObjectId
    commentee_id: ObjectId
    content: str
    created_on: datetime = datetime.utcnow


class UserInfo(EmbeddedModel):
    id: ObjectId
    name: str
    avatar_url: str


class Post(Model):
    _id: ObjectId = ObjectId
    user: UserInfo
    title: str
    intro: str
    cover_url: str
    content: str
    visible: bool = False
    can_comment: bool = True
    comments: List[Comment]
    created_on: datetime = datetime.utcnow
    updated_on: datetime = datetime.utcnow

    class Meta:
        # required = ['user', 'title', 'content']
        indexes = ['title']

    @classmethod
    def find_all_visible_posts_of_user(cls, uid: ObjectId):
        return Post.find({'user_id': uid, 'visible': True})

    def __str__(self):
        return '<Post: {}>'.format(self.title)


Post.set_db(MongoClient().get_database('daydream-dev'))
# user = UserInfo(id=ObjectId(), name='rina')
# post = Post(user=user, title='hello')
#
# print(post.user == post.user)

post = Post.find_one()
print(post.title)
