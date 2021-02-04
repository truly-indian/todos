package com.deepak.rapido.de_project;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;


public class MainVerticle extends AbstractVerticle {


  //private static Logger LOG = LoggerFactory.getLogger(MainVerticle.class);
  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    JsonObject config = new JsonObject().put("connection_string","mongodb://apiUser2:password@172.30.236.8:27017/deepak?authSource=deepak&replicaSet=rapidoReplSet");
    MongoClient mongoClient = MongoClient.create(vertx, config);
    Router todos = Router.router(vertx);
    todos.route().handler(BodyHandler.create());
    //Route for getting all the todos;
    todos.get("/todos").handler(req -> {
      JsonObject query = new JsonObject();
      mongoClient.find("todos",query,listAsyncResult -> {
        if (listAsyncResult.succeeded()) {
          JsonArray all = new JsonArray();
          System.out.println(listAsyncResult);
          for(JsonObject json: listAsyncResult.result()) {
             all.add(JsonObject.mapFrom(json));
          }
          System.out.println(listAsyncResult);
          req.response().putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
            .end(all.encodePrettily());
        } else {
          listAsyncResult.cause().printStackTrace();
        }
      });
    });
    // route for getting a specifictodo by priority
    todos.get("/todos/:id").handler(req -> {
       final String qid = req.pathParam("id");
       final Double qqid = Double.parseDouble(qid);
      JsonObject query = new JsonObject();
      query.put("priority",qqid);
      mongoClient.find("todos",query, listAsyncResult -> {
        if (listAsyncResult.succeeded()) {
          JsonObject jobj = new JsonObject();
          for (JsonObject json : listAsyncResult.result()) {
           // System.out.println(json.encodePrettily());
            jobj = JsonObject.mapFrom(json);
            //not able to map this object
          }
         // System.out.println(jobj);
          req.response().putHeader(HttpHeaders.CONTENT_TYPE,HttpHeaderValues.APPLICATION_JSON)
            .end(JsonObject.mapFrom(jobj).encodePrettily());
        } else {
          listAsyncResult.cause().printStackTrace();
        }
      });
    });

    //route for posting atodo
    todos.post("/todos").handler(req -> {
      final JsonObject requestBody =  req.getBodyAsJson();
      System.out.println(requestBody);
      mongoClient.save("todos",requestBody,stringAsyncResult -> {
        if (stringAsyncResult.succeeded()) {
          String id = stringAsyncResult.result();
          //System.out.println("Saved book with id " + id);
          req.response().putHeader(HttpHeaders.CONTENT_TYPE,HttpHeaderValues.APPLICATION_JSON)
            .end(new JsonObject().put("message",id).encodePrettily());
        } else {
          stringAsyncResult.cause().printStackTrace();
        }
      });
    });
    //route for deleting atodo
    todos.delete("/todos/:id").handler(req -> {
       final String id = req.pathParam("id");
      JsonObject query = new JsonObject()
        .put("_id",id);
      mongoClient.removeDocuments("todos", query, res -> {
        if (res.succeeded()) {
          System.out.println("Never much liked Tolkien stuff!");
          req.response().putHeader(HttpHeaders.CONTENT_TYPE,HttpHeaderValues.APPLICATION_JSON)
            .end(new JsonObject().put("message","Deleted the record todo successfully:)").encodePrettily());
        } else {
          res.cause().printStackTrace();
        }
      });
    });
    //route for updating the document
    todos.put("/todos/:id").handler(req -> {
      final String id = req.pathParam("id");
      JsonObject query = new JsonObject()
        .put("_id", id);
      JsonObject update = new JsonObject().put("$set", new JsonObject()
        .put("priority", 3));
      mongoClient.updateCollection("todos", query, update, res -> {
        if (res.succeeded()) {
          System.out.println("Book updated !");
          req.response().putHeader(HttpHeaders.CONTENT_TYPE,HttpHeaderValues.APPLICATION_JSON)
            .end(new JsonObject().put("message",id).encodePrettily());
        } else {
          res.cause().printStackTrace();
        }
      });
    });

    vertx.createHttpServer().requestHandler(todos).listen(8080, http -> {
      if (http.succeeded()) {
        startPromise.complete();
        System.out.println("HTTP server started on port 8080");
      } else {
        startPromise.fail(http.cause());
      }
    });
  }

  public static void main(String[] args) {
    System.out.println("Newly started");
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(new MainVerticle());
  }
}
