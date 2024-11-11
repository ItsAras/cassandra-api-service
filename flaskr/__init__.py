from flask import (Flask, request, jsonify)
import json
import pymongo
import werkzeug
from bson.objectid import ObjectId
from cassandra.cluster import Cluster

def create_app():
    app = Flask(__name__)
    cassandraClient = Cluster()
    session = cassandraClient.connect()
    
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS warehouse
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
        """
    )
    
    session.set_keyspace('warehouses')
    
    session.execute(
    """
    CREATE TABLE IF NOT EXISTS warehouses (
        id TEXT PRIMARY KEY,
        name TEXT,
        location TEXT
    );
    """
    )
    
    session.execute(
    """
    CREATE TABLE IF NOT EXISTS products (
        id TEXT PRIMARY KEY,
        amount INT,
        description TEXT,
        category TEXT,
        warehouseId TEXT
    );
    """
    )

    session.execute(
        """
        CREATE INDEX IF NOT EXISTS ON products (warehouseId);
        """
    )
    
    session.execute(
        """
        CREATE INDEX IF NOT EXISTS ON products (category);
        """
    )
    
    @app.route("/warehouses", methods=["PUT"])
    def register_warehouse():
        reqBody = request.json
        
        session.execute(
        """
        INSERT INTO warehouses (id, name, location)
        VALUES (%s, %s, %s)
        """,
        (reqBody.get("id"), reqBody.get("name"), reqBody.get("location"))
        )

        return { "id": reqBody.get("id") }, 201
    
    @app.route("/warehouses", methods=["GET"])
    def get_warehouses():    
        warehouseList = []
        
        warehouses = session.execute(
        """
        SELECT id, name, location
        FROM warehouses
        """
        )
        
        for warehouse in warehouses:
            warehouseList.append({
                "id": warehouse.id,
                "name": warehouse.name,
                "location": warehouse.location
            })
            
        return warehouseList, 200
    
    @app.route("/warehouses/<warehouseId>", methods=["GET"])
    def get_warehouse(warehouseId):    
        warehouse = session.execute(
        """
        SELECT id, name, location
        FROM warehouses
        WHERE id = %s
        """,
        (warehouseId,)
        ).one()
        
        if warehouse:
            warehouse = {
                "id": warehouse.id,
                "name": warehouse.name,
                "location": warehouse.location
            }
            
            return warehouse, 200
        
        else:
            return { "message": "Warehouse not found." }, 404
        
    @app.route("/warehouses/<warehouseId>", methods=["DELETE"])
    def delete_warehouse(warehouseId):
        warehouse = session.execute(
            """
            SELECT id FROM warehouses
            WHERE id = %s
            """,
            (warehouseId,)
        ).one()
        
        if warehouse:
            session.execute(
                """
                DELETE FROM warehouses
                WHERE id = %s
                """,
                (warehouseId,)
            )
            
            return { "message": "Warehouse deleted." }, 200
        
        else:
            return { "message": "Warehouse not found." }, 404
    
    @app.route("/warehouses/<warehouseId>/inventory", methods=["PUT"])
    def add_product_to_warehouse_inventory(warehouseId):
        reqBody = request.json
        
        warehouse = session.execute(
            """
            SELECT id FROM warehouses
            WHERE id = %s
            """,
            (warehouseId,)
        ).one()
        
        if warehouse:   
            session.set_keyspace('products')
            session.execute(
                """
                INSERT INTO products (id, amount, description, category, warehouseId)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (reqBody.get("id"), reqBody.get("amount"), reqBody.get("description"), reqBody.get("category"), warehouseId)
            )
            
            return { "message": "Product added to inventory." }, 201
        
        else:
            return { "message": "Warehouse not found." }, 404
        
    @app.route("/warehouses/<warehouseId>/inventory", methods=["GET"])
    def list_warehouse_inventory(warehouseId):
        category = request.args.get('category')
        productList = []
        
        if category:  
            products = session.execute(
            """
            SELECT id, amount, description, category
            FROM products
            WHERE warehouseId = %s AND category = %s
            """,
            (warehouseId, category)
            )
            
        else:
            products = session.execute(
            """
            SELECT id, amount, description, category
            FROM products
            WHERE warehouseId = %s
            """,
            (warehouseId,)
            )
        
        for product in products:
            productList.append({
                "id": product.id,
                "amount": product.amount,
                "description": product.description,
                "category": product.category
        })
        
        return productList, 200
        
    @app.route("/warehouses/<warehouseId>/inventory/<inventoryId>", methods=["GET"])
    def get_warehouse_inventory_product(warehouseId, inventoryId):

        return 'productList', 201
        
    return app