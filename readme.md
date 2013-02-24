
#### Chronicle Demo

## Plan

Teleport the index and headers to a second jvm using chronicle and so that there is master and slave and run sync on slave. 

Verify the random access file store; instrument the file access to and write tests which throw exceptions and prove it is crash proof. 

Add a recover and compact method. Speed up the binary writes. 

## Write Up

This demo app is ...

## Build

Build and run the code with: 

	mvn package

You can set the following properties with defaults as shown: 

	-Dxxx=yyy 

The defaults are configured within blahblah.properties

Upon first startup the application will...

Note that the code assumes ...

## Running On Redhat Openshift PaaS Cloud

ToDo
	
## Inspiration 

http://www.javaworld.com/jw-01-1999/jw-01-step.html?page=1

ToDo

End.
