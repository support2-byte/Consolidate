# Logistics Management System API

## Overview
This is a Node.js Express backend API for a logistics/shipping management system. It provides REST endpoints for managing customers, vendors, containers, orders, and consignments with JWT-based authentication.

## Project Architecture
- **Backend**: Node.js with Express.js framework
- **Database**: PostgreSQL (external Neon database)
- **Authentication**: JWT tokens with bcrypt password hashing
- **Port**: Runs on port 3000 (backend API)

## Key Features
- User authentication (register, login, logout)
- CRUD operations for:
  - Customers (name, email, phone)
  - Vendors (name, contact_person, phone)
  - Containers (container_number, type, status)
  - Orders (customer_id, order_date, status, total)
  - Consignments (order_id, container_id, shipment_date, status)

## Database Tables Created
- `users` - Authentication users
- `customers` - Customer information
- `vendors` - Vendor/supplier details
- `containers` - Shipping container tracking
- `orders` - Customer orders with foreign key to customers
- `consignments` - Shipment tracking with foreign keys to orders and containers

## Environment Configuration
- Server runs on localhost:3000 (backend)
- CORS configured for development and Replit domains
- Database connection established with external Neon PostgreSQL

## Recent Changes
- Configured server for Replit environment (localhost, port 3000)
- Updated CORS to allow Replit proxy domains
- Created all required database tables with proper relationships
- Set up workflow to run Node.js server
- Configured deployment as VM type for stateful backend

## Workflow
- Server runs via `npm run dev` with PORT=3000
- Uses nodemon for development auto-restart

## API Endpoints
- `POST /auth/register` - User registration
- `POST /auth/login` - User login
- `GET /auth/me` - Get current user
- `POST /auth/logout` - User logout
- `GET /api/customers` - List customers (authenticated)
- `POST /api/customers` - Create customer (authenticated)
- `PUT/DELETE /api/customers/:id` - Update/delete customer (authenticated)
- Similar CRUD endpoints for vendors, containers, orders, consignments
- `GET /health` - Health check endpoint

## Current Status
✅ Database connected and tables created
✅ Server configured for Replit environment
✅ Workflow set up (though shows as failed in UI, server is actually running)
✅ Deployment configuration completed
✅ All required dependencies installed