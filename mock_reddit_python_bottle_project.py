import pymongo
from pymongo import MongoClient
from bson.json_util import dumps
import bottle
from bottle import route, run, response,abort,request
from bson.objectid import ObjectId
import re, json

regex = re.compile(".*");
HOT_TOPICS_VOTE_FLAG = int(5)

#getConnection with mongoDB
client = MongoClient('localhost', 27017)

#get db
db = client.reddit1_db

##################### get_all_topics() METHOD ########################
@route('/topics', method='GET')
def get_all_topics():
    try:
        result = json.dumps(get_all_topics_from_mongo())
        stats = 200
    except:
        result = ''
        stats = 500
    return bottle.HTTPResponse(status=stats, body=result, content_type='application/json')

##################### delete_topic(topicId) METHOD ########################
@route('/topics/<topicId>', method='DELETE')
def delete_topic(topicId):
    print 'Delete topicId : '+topicId
    if topicId is None or len(str(topicId)) == 0:
        statusCode = 400
        resultMsg = 'No topic id provided...'
        return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')
        #print 'None topicId'
    else:
        try:
            delete_topic_from_mongodb(topicId)
            statusCode = 200
            print 'delete status is 200'
            resultMsg = 'The topic you wanted to be deleted, has been deleted successfully...'
            return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')
        except:
            statusCode = 500
            resultMsg = 'Internal server error occured while attempting to delete the topic...'
            return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')
            #print 'delete status is 500'
            #print 'status is : '+stats
    #return bottle.HTTPResponse(status=stats)

##################### get_all_topics_from_mongo() METHOD ########################
def get_all_topics_from_mongo():
    results = db.reddit1_collection.find({"topicName":regex},{"topicName":1,"author":1,"upVoteCount":1,"downVoteCount":1,"_id":1})
    #results = db.reddit1_collection.find()
    final = "["
    for key in results:
        print str(key).replace('_id','topicId')
        final=final+str(key).replace('_id','topicId')+","
    final=final+"]"
    final=final.replace(",]","]")
    return final

##################### delete_topic_from_mongodb(topicId) METHOD ########################
def delete_topic_from_mongodb(topicId):
    results = db.reddit1_collection.remove({'_id':ObjectId(topicId)})
    return results

##################### create_topic() METHOD ########################
@route('/topics', method= 'POST')
def create_topic():
    data = request.body.readline()
    if not data:
        statusCode = 400
        resultMsg = 'Bad request, No data received'
        return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')
	#if topicId is None or len(str(topicId)) == 0:
	
    entity = json.loads(data)
    print entity
    if entity['topicName'] is None or len(entity['topicName']) == 0:
        statusCode = 400
	resultMsg = 'Bad request, No topic name provided'
        return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')
    if entity['desc'] is None or len(entity['desc']) == 0:
	statusCode = 400
	resultMsg = 'Bad request, No description received'
	return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')
    if entity['author'] is None or len(entity['author']) == 0:
	statusCode = 400
	resultMsg = 'Bad request, No author specified'
	return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')
    try:
	newTopicId = db['reddit1_collection'].insert(entity)
	print 'New topic created: '  + str(newTopicId)
	#return '{topicId: "'+ str(newTopicId) + '"}'
	statusCode = 200
	resultMsg = 'Your topic has been created and the Topic ID for the same is '+str(newTopicId)+'...'
	return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')
    except ValidationError as ve:
	statusCode = 500
	#resultMsg = 'Error occured while attempting to create your topic. Please try after a while!!!'
	resultMsg = str(ve)
	return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')

##################### view_topic(topicId) METHOD ########################
@route('/topics/<topicId>',method = 'GET')
def view_topic(topicId):
    try:
        entity = db.reddit1_collection.find({'_id' : ObjectId(topicId)},{"topicName" : 1, "author" : 1, "comments": 1, "desc" : 1, "upVoteCount" : 1, "downVoteCount" : 1, "_id" : 0})
	if not entity:
	    statusCode = 400
	    resultMsg = 'Sorry!!! We cannot find any topic by this ID...'
	    return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')
	else:
	    statusCode = 200
	    result = dumps(entity)
	    print entity
	    return bottle.HTTPResponse(status = statusCode, body = result,content_type='application/json')
    except:
	result = "No Result found "
	stats = 500
        return bottle.HTTPResponse(status = stats, body = result,content_type='application/json')
        #return result

##################### comment_on_topic_by_topicid(topicId) METHOD ########################
#The method for adding a comment for topic. In this method we take the topicId and
#then add comment under that topic.
@route('/topics/<topicId>/comments', method='POST')
def comment_on_topic_by_topicid(topicId):
    print('The comment would be added to the topic with topicId :: ', topicId)
    
    #Check if the jSon request body is complete and has all the paramteres required.
    #For this let us read the request.
    
    requestData = json.loads(request.body.readline())
    print('The contents of request are :: ', requestData)
    
    #Check if the requestData is present that is not null
    #if not, we would say not enough parameters in the request
    
    if not requestData:
        print('Some paramters are missing in the received request... Throwing the exception...')
        statusCode = 400
        resultMsg = 'Sorry we cannot the request... Make sure to pass required parameters (author and comment)'
        return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')
        
    else:
        #If we know requestData is not null, let us check if the provided topicId does exist in MongoDB.
        #So first try and get the topic from DB by topicId
        
        availableTopic = db['reddit1_collection'].find_one({'_id':ObjectId(topicId)})
        print('The info about topic', availableTopic)
           
        if availableTopic:
            print('Yes the topic is present... We can add the comment')
            
            #Load the request body for further verification            
            #Check if author key is present in request
            
            if not requestData.has_key('author'):
                print('Key author not specified... Cannot process the request')
                statusCode = 400
                resultMsg = 'Key author not specified... Cannot process the request'
                return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')
                
            #Check if comment is added in the request
            
            if not requestData.has_key('comment'):
                print("Key comment not specified... Cannot process the request")
                statusCode = 400
                resultMsg = 'Key comment not specified... Cannot process the request'
                return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')
            
            #If everything OK, proceed by adding the comment to the topic.
            #Let us first bubild the comment.
            
            comments = {'author': requestData['author'],'comment': requestData['comment']}
            
            #Now, we add the comment to the topic with respect to topicId.
            
            try:
                db['reddit1_collection'].update({"_id": ObjectId(topicId)}, {'$addToSet':{'comments':comments}})
                statusCode = 200
		resultMsg = 'Your comment has been added to the topic...'
                return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')
            except ValidationError as ve:
                statusCode = 500
                #resultMsg = 'Internal server error occured while trying to add your comment to the topic...'
                resultMsg = str(ve)
                return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')
        else:
            print('Sorry the topic is not present... We cannot process the request...')
            statusCode = 400
            resultMsg = 'Sorry we cannot find a topic by this topicId... We cannot process the request...'
            return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')

##################### update_topic_with_upvote_downvote(topicId) METHOD ########################
#Method update the topic with upVote or downVote. For upVote, we increment upVoteCount
#For downVote, we increment the downVoteCount.
@route('/topics/<topicId>/votes', method='POST')
def update_topic_with_upvote_downvote(topicId):
    print('The vote would be added to the topic with topicId :: ', topicId)
    
    #Check if the jSon request body is complete and has all the paramteres required.
    #For this let us read the request.
    
    requestData = json.loads(request.body.readline())
    print('The contents of request are :: ', requestData)
    
    #Check if the requestData is present that is not null
    #if not, we would say not enough parameters in the request
    
    if not requestData:
        print('You did not provide enough paramaters!!!...')
        statusCode = 400
        resultMsg = 'You did not provide enough paramaters!!!... Provide type of vote...'
        return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')
    
    else:
        #If we know requestData is not null, let us check if the provided topicId does exist in MongoDB.
        #So first try and get the topic from DB by topicId
        
        availableTopic = db['reddit1_collection'].find_one({'_id':ObjectId(topicId)})
        print('The info about topic', availableTopic)
        
        if availableTopic:
	    print('Yes the topic is present... We can add the vote')
	    
	    #Load the request body for further verification            
            #Check if vote key is present in request
            
            if not requestData.has_key('vote'):
	        print('Key (vote) not specified... Cannot process the request')
	        statusCode = 400
	        resultMsg = 'Key (vote) not specified... Cannot process the request'
                return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')
            
            #If everything OK, proceed by adding the vote to the topic.
	    #Let us first build the appropriate voteCounter based on type of vote and try adding to db.
	    try:
	        if requestData['vote'] == str('1'):
	            db['reddit1_collection'].update({"_id": ObjectId(topicId)}, {'$inc':{'upVoteCount': int(1)}})
	            statusCode = 200
	    	    resultMsg = 'Your voting has been added to the topic...'
	            return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')
	        if requestData['vote'] == str('-1'):
	            db['reddit1_collection'].update({"_id": ObjectId(topicId)}, {'$inc':{'downVoteCount': int(1)}})
	            statusCode = 200
	    	    resultMsg = 'Your voting has been added to the topic...'
	            return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')
	    except Exception as e:
	        statusCode = 500
	        #resultMsg = 'Internal server error occured while trying to add your vote to the topic...'
	        resultMsg = str(e)
                return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')
        else:
	    print('Sorry the topic is not present... We cannot process the request...')
	    statusCode = 400
	    resultMsg = 'Sorry we cannot find a topic by this topicId... We cannot process the request...'
            return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')
	    
####################### list_hot_topics() ####################
#This method is similar to get_all_topics_from_mongo() except that it would list only the hot topics from the DB.
#We define a topic as hot, if it holds either upVoteCount or downVoteCount = HOT_TOPICS_VOTE_FLAG declared above
@route('/topics/hot', method='GET')
def list_hot_topics():
    print 'Inside the list hot topics service...'
    try:
        entity = db.reddit1_collection.find({'$or': [{'upVoteCount':{'$gt':HOT_TOPICS_VOTE_FLAG}}, {'downVoteCount':{'$gt':HOT_TOPICS_VOTE_FLAG}}]})
        if not entity:
            statusCode = 400
            resultMsg = 'Sorry!!! At present we do not seem to have trending topic... Why not Vote one topic...'
            return bottle.HTTPResponse(status = statusCode, body = resultMsg,content_type='application/json')
        else:
            statusCode = 200
            result = dumps(entity)
            print entity
            return bottle.HTTPResponse(status = statusCode, body = result,content_type='application/json')
    except:
        result = "No Result found "
        stats = 500
        return bottle.HTTPResponse(status = stats, body = result,content_type='application/json')

run(host='localhost', port=8080, debug=True)