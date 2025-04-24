# SCD (Slowly Changing Dimentsion)
In the current project *gorm* as the ORM layer has been used.  
You can use only two function *Read* and *Write* respectively.  
Before invoking any of the funcnality you have to provide the config so that application can fetch version and id field respectively.  

## For testing
In order to test the above package you'll be requiring *Docker* because we are relying on **testcontainers** for the test db.  
