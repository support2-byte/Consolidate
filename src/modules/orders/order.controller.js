import pool from "../../db/pool.js";

// Date validation helpers (unchanged)
function isValidDate(dateString) {
  if (!dateString) return false;
  const normalized = dateString.toString().split('T')[0];  // Strip time if full ISO
  const date = new Date(normalized);
  return !isNaN(date.getTime()) && normalized.match(/^\d{4}-\d{2}-\d{2}$/);
}

function normalizeDate(dateString) {
  if (!dateString) return null;
  const normalized = dateString.toString().split('T')[0];
  return isValidDate(normalized) ? normalized : null;
}

// Updated validation: Distinguish missing vs. invalid; cleaner messages
function validateOrderFields({
  bookingRef, status, eta, etd, placeOfLoading, finalDestination, placeOfDelivery,
  senderName, receiverName, shippingLine
}) {
  const errors = [];
  // Required non-dates: Check if missing
  if (!bookingRef) errors.push('booking_ref required');
  if (!status) errors.push('status required');
  if (!placeOfLoading) errors.push('place_of_loading required');
  if (!finalDestination) errors.push('final_destination required');
  if (!placeOfDelivery) errors.push('place_of_delivery required');
  if (!senderName) errors.push('sender_name required');
  if (!receiverName) errors.push('receiver_name required');
  if (!shippingLine) errors.push('shipping_line required');

  // Required dates: Check if missing or invalid
  if (eta === undefined || eta === null || eta === '') {
    errors.push('eta required');
  } else if (!isValidDate(eta)) {
    errors.push(`eta invalid format (got: "${eta}")`);
  }
  if (etd === undefined || etd === null || etd === '') {
    errors.push('etd required');
  } else if (!isValidDate(etd)) {
    errors.push(`etd invalid format (got: "${etd}")`);
  }

  return errors;
}

// Helper for order status color mapping (unchanged)
function getOrderStatusColor(status) {
  const colors = {
    'Created': 'info',
    'In Transit': 'warning',
    'Delivered': 'success',
    'Cancelled': 'error'
  };
  return colors[status] || 'default';
}

// Map order status to container availability (unchanged)
export function getContainerAvailability(orderStatus) {
  switch (orderStatus) {
    case 'Created': return 'Assigned to Job';
    case 'In Transit': return 'In Transit';
    case 'Delivered': return 'Available';
    case 'Cancelled': return 'Available';
    default: return 'Available';
  }
}

// Create a new order (defaults for all vars; log values.length)
export async function createOrder(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');

    const updates = req.body || {};
    const files = req.files || {}; // Assuming multer .fields() or .any()
    const created_by = 'system';

    // Debug log
    console.log('Order create body (key fields):', { booking_ref: updates.booking_ref, status: updates.status, eta: updates.eta, etd: updates.etd, shipping_line: updates.shipping_line });
    console.log('Files received:', Object.keys(files));

    // Parse existing attachments and gatepass if provided in form (for consistency, though typically empty for create)
    let currentAttachments = [];
    let currentGatepass = [];

    // Handle attachments
    let newAttachments = currentAttachments;
    let existingAttachmentsFromForm = [];
    if (updates.attachments_existing) {
      try {
        existingAttachmentsFromForm = JSON.parse(updates.attachments_existing);
      } catch (e) {
        console.warn('Failed to parse attachments_existing:', e.message);
      }
    }
    if (existingAttachmentsFromForm.length > 0) {
      newAttachments = existingAttachmentsFromForm; // Use form's existing if provided (e.g., text-only create with presets)
    }
    if (files.attachments && files.attachments.length > 0) {
      const uploadedPaths = await uploadFiles(files.attachments, 'attachments'); // Implement uploadFiles to save to disk/S3 and return paths
      newAttachments = [...newAttachments, ...uploadedPaths];
    }
    const attachmentsJson = JSON.stringify(newAttachments);

    // Handle gatepass (similarly)
    let newGatepass = currentGatepass;
    let existingGatepassFromForm = [];
    if (updates.gatepass_existing) {
      try {
        existingGatepassFromForm = JSON.parse(updates.gatepass_existing);
      } catch (e) {
        console.warn('Failed to parse gatepass_existing:', e.message);
      }
    }
    if (existingGatepassFromForm.length > 0) {
      newGatepass = existingGatepassFromForm; // Use form's existing if provided
    }
    if (files.gatepass && files.gatepass.length > 0) {
      const uploadedPaths = await uploadFiles(files.gatepass, 'gatepass');
      newGatepass = [...newGatepass, ...uploadedPaths];
    }
    const gatepassJson = JSON.stringify(newGatepass);

    // Allowed update fields (now includes attachments, gatepass)
    const allowedUpdateFields = [
      'booking_ref', 'status', 'eta', 'etd', 'place_of_loading', 'final_destination',
      'place_of_delivery', 'sender_name', 'receiver_name', 'shipping_line',
      'associated_container', 'consignment_remarks', 'rgl_booking_number',
      'order_remarks', 'consignment_number', 'consignment_vessel',
      'consignment_voyage', 'sender_contact', 'sender_address', 'sender_email',
      'receiver_contact', 'receiver_address', 'receiver_email', 'driver_name',
      'driver_contact', 'driver_nic', 'driver_pickup_location', 'truck_number',
      'third_party_transport', 'category', 'subcategory', 'type', 'delivery_address',
      'pickup_location', 'weight', 'drop_method', 'drop_off_cnic', 'drop_off_mobile',
      'plate_no', 'drop_date', 'collection_method', 'full_partial', 'qty_delivered',
      'client_receiver_name', 'client_receiver_id', 'client_receiver_mobile',
      'delivery_date', 'attachments', 'gatepass'
    ];

    // Date keys to exclude from text fields
    const dateKeys = ['eta', 'etd', 'drop_date', 'delivery_date'];

    // Validation (only for provided fields)
    const updatedFields = {
      bookingRef: updates.booking_ref,
      status: updates.status,
      eta: updates.eta,
      etd: updates.etd,
      placeOfLoading: updates.place_of_loading,
      finalDestination: updates.final_destination,
      placeOfDelivery: updates.place_of_delivery,
      senderName: updates.sender_name,
      receiverName: updates.receiver_name,
      shippingLine: updates.shipping_line,
      rglBookingNumber: updates.rgl_booking_number,
      consignmentRemarks: updates.consignment_remarks,
      orderRemarks: updates.order_remarks,
      consignmentNumber: updates.consignment_number,
      consignmentVessel: updates.consignment_vessel,
      consignmentVoyage: updates.consignment_voyage,
      senderContact: updates.sender_contact,
      senderAddress: updates.sender_address,
      senderEmail: updates.sender_email,
      receiverContact: updates.receiver_contact,
      receiverAddress: updates.receiver_address,
      receiverEmail: updates.receiver_email,
      driverName: updates.driver_name,
      driverContact: updates.driver_contact,
      driverNic: updates.driver_nic,
      driverPickupLocation: updates.driver_pickup_location,
      truckNumber: updates.truck_number,
      thirdPartyTransport: updates.third_party_transport,
      category: updates.category,
      subcategory: updates.subcategory,
      type: updates.type,
      deliveryAddress: updates.delivery_address,
      pickupLocation: updates.pickup_location,
      weight: updates.weight,
      dropMethod: updates.drop_method,
      dropOffCnic: updates.drop_off_cnic,
      dropOffMobile: updates.drop_off_mobile,
      plateNo: updates.plate_no,
      dropDate: updates.drop_date,
      collectionMethod: updates.collection_method,
      fullPartial: updates.full_partial,
      qtyDelivered: updates.qty_delivered,
      clientReceiverName: updates.client_receiver_name,
      clientReceiverId: updates.client_receiver_id,
      clientReceiverMobile: updates.client_receiver_mobile,
      deliveryDate: updates.delivery_date,
    };
    const updateErrors = [];
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    const mobileRegex = /^\d{10,15}$/;

    // Helper to get actual field name (snake_case)
    const getActualField = (camelField) => {
      switch (camelField) {
        case 'bookingRef': return 'booking_ref';
        case 'rglBookingNumber': return 'rgl_booking_number';
        case 'placeOfLoading': return 'place_of_loading';
        case 'finalDestination': return 'final_destination';
        case 'placeOfDelivery': return 'place_of_delivery';
        case 'senderName': return 'sender_name';
        case 'receiverName': return 'receiver_name';
        case 'shippingLine': return 'shipping_line';
        case 'consignmentRemarks': return 'consignment_remarks';
        case 'orderRemarks': return 'order_remarks';
        case 'consignmentNumber': return 'consignment_number';
        case 'consignmentVessel': return 'consignment_vessel';
        case 'consignmentVoyage': return 'consignment_voyage';
        case 'senderContact': return 'sender_contact';
        case 'senderAddress': return 'sender_address';
        case 'senderEmail': return 'sender_email';
        case 'receiverContact': return 'receiver_contact';
        case 'receiverAddress': return 'receiver_address';
        case 'receiverEmail': return 'receiver_email';
        case 'driverName': return 'driver_name';
        case 'driverContact': return 'driver_contact';
        case 'driverNic': return 'driver_nic';
        case 'driverPickupLocation': return 'driver_pickup_location';
        case 'truckNumber': return 'truck_number';
        case 'thirdPartyTransport': return 'third_party_transport';
        case 'deliveryAddress': return 'delivery_address';
        case 'pickupLocation': return 'pickup_location';
        case 'dropMethod': return 'drop_method';
        case 'dropOffCnic': return 'drop_off_cnic';
        case 'dropOffMobile': return 'drop_off_mobile';
        case 'plateNo': return 'plate_no';
        case 'dropDate': return 'drop_date';
        case 'collectionMethod': return 'collection_method';
        case 'fullPartial': return 'full_partial';
        case 'qtyDelivered': return 'qty_delivered';
        case 'clientReceiverName': return 'client_receiver_name';
        case 'clientReceiverId': return 'client_receiver_id';
        case 'clientReceiverMobile': return 'client_receiver_mobile';
        case 'deliveryDate': return 'delivery_date';
        default: return camelField.toLowerCase().replace(/([A-Z])/g, '_$1').toLowerCase();
      }
    };

    // Required fields validation
    const requiredFields = [
      'bookingRef',
      'rglBookingNumber',
      'senderName',
      'receiverName',
      'placeOfLoading',
      'finalDestination',
      'category',
      'subcategory',
      'type',
      'weight'
    ];

    requiredFields.forEach(camelField => {
      const value = updatedFields[camelField];
      if (!value || !value.trim()) {
        const actualField = getActualField(camelField);
        updateErrors.push(`${actualField} is required`);
      }
    });

    // Conditional validations
    const showInbound = updates.final_destination && updates.final_destination.includes('Karachi');
    const showOutbound = updates.place_of_loading && updates.place_of_loading.includes('Dubai');

    if (showInbound && updatedFields.dropMethod === 'Drop-Off') {
      if (!updatedFields.dropOffCnic || !updatedFields.dropOffCnic.trim()) updateErrors.push('drop_off_cnic required for Drop-Off');
      if (!updatedFields.dropOffMobile || !updatedFields.dropOffMobile.trim()) updateErrors.push('drop_off_mobile required for Drop-Off');
    }
    if (showInbound && (!updatedFields.dropDate || !updatedFields.dropDate.trim())) {
      updateErrors.push('drop_date required');
    }

    if (showOutbound && (!updatedFields.deliveryDate || !updatedFields.deliveryDate.trim())) {
      updateErrors.push('delivery_date required');
    }
    if (showOutbound && updatedFields.fullPartial === 'Partial' && (!updatedFields.qtyDelivered || !updatedFields.qtyDelivered.trim())) {
      updateErrors.push('qty_delivered required for Partial delivery');
    }
    if (showOutbound && updatedFields.collectionMethod === 'Collected by Client') {
      if (!updatedFields.clientReceiverName || !updatedFields.clientReceiverName.trim()) updateErrors.push('client_receiver_name required for Client Collection');
      if (!updatedFields.clientReceiverId || !updatedFields.clientReceiverId.trim()) updateErrors.push('client_receiver_id required for Client Collection');
      if (!updatedFields.clientReceiverMobile || !updatedFields.clientReceiverMobile.trim()) updateErrors.push('client_receiver_mobile required for Client Collection');
    }

    // Format validations (only if provided and not empty)
    for (const [camelField, value] of Object.entries(updatedFields)) {
      if (value !== undefined && value !== null && value.trim() !== '') {
        const actualField = getActualField(camelField);

        if (['eta', 'etd', 'dropDate', 'deliveryDate'].includes(camelField) && !isValidDate(value)) {
          updateErrors.push(`${actualField} invalid date format (use YYYY-MM-DD)`);
        } else if (camelField === 'weight' && (isNaN(value) || parseFloat(value) <= 0)) {
          updateErrors.push(`${actualField} must be a positive number`);
        } else if (['senderEmail', 'receiverEmail'].includes(camelField) && !emailRegex.test(value)) {
          updateErrors.push(`${actualField} invalid email format`);
        } else if (['dropOffMobile', 'clientReceiverMobile'].includes(camelField) && !mobileRegex.test(value.replace(/\D/g, ''))) {
          updateErrors.push(`${actualField} invalid mobile number (10-15 digits expected)`);
        }
        // Add more specific validations as needed (e.g., enums for status, drop_method, etc.)
      }
    }

    if (updateErrors.length > 0) {
      console.warn('Create validation failed:', updateErrors);
      await client.query('ROLLBACK');
      return res.status(400).json({ 
        error: 'Invalid create fields',
        details: updateErrors.join('; ')
      });
    }

    // Normalize dates
    const normEta = updates.eta ? normalizeDate(updates.eta) : null;
    const normEtd = updates.etd ? normalizeDate(updates.etd) : null;
    const normDropDate = updates.drop_date ? normalizeDate(updates.drop_date) : null;
    const normDeliveryDate = updates.delivery_date ? normalizeDate(updates.delivery_date) : null;

    if ((updates.eta && !normEta) || (updates.etd && !normEtd) || (updates.drop_date && !normDropDate) || (updates.delivery_date && !normDeliveryDate)) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'Invalid date format. Use YYYY-MM-DD.' });
    }

    const query = `
      INSERT INTO orders (
        booking_ref, status, rgl_booking_number, consignment_remarks,
        place_of_loading, final_destination, place_of_delivery, order_remarks,
        associated_container, consignment_number, consignment_vessel, consignment_voyage,
        sender_name, sender_contact, sender_address, sender_email,
        receiver_name, receiver_contact, receiver_address, receiver_email,
        eta, etd, shipping_line,
        driver_name, driver_contact, driver_nic, driver_pickup_location, truck_number,
        third_party_transport, category, subcategory, type, delivery_address, pickup_location, weight,
        drop_method, drop_off_cnic, drop_off_mobile, plate_no, drop_date,
        collection_method, full_partial, qty_delivered, client_receiver_name, client_receiver_id,
        client_receiver_mobile, delivery_date, attachments, gatepass, created_by
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32,
                $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50)
      RETURNING *
    `;

    const values = [
      updates.booking_ref || '',
      updates.status || 'In Transit',
      updates.rgl_booking_number || '',
      updates.consignment_remarks || '',
      updates.place_of_loading || '',
      updates.final_destination || '',
      updates.place_of_delivery || '',
      updates.order_remarks || '',
      updates.associated_container || '',
      updates.consignment_number || '',
      updates.consignment_vessel || '',
      updates.consignment_voyage || '',
      updates.sender_name || '',
      updates.sender_contact || '',
      updates.sender_address || '',
      updates.sender_email || '',
      updates.receiver_name || '',
      updates.receiver_contact || '',
      updates.receiver_address || '',
      updates.receiver_email || '',
      normEta,
      normEtd,
      updates.shipping_line || '',
      updates.driver_name || '',
      updates.driver_contact || '',
      updates.driver_nic || '',
      updates.driver_pickup_location || '',
      updates.truck_number || '',
      updates.third_party_transport || '',
      updates.category || '',
      updates.subcategory || '',
      updates.type || '',
      updates.delivery_address || '',
      updates.pickup_location || '',
      updates.weight || null,
      updates.drop_method || '',
      updates.drop_off_cnic || '',
      updates.drop_off_mobile || '',
      updates.plate_no || '',
      normDropDate,
      updates.collection_method || '',
      updates.full_partial || '',
      updates.qty_delivered || null,
      updates.client_receiver_name || '',
      updates.client_receiver_id || '',
      updates.client_receiver_mobile || '',
      normDeliveryDate,
      attachmentsJson,
      gatepassJson,
      created_by
    ];

    // Debug: Log length to confirm match
    console.log('Query columns: 50, Values length:', values.length);

    const result = await client.query(query, values);
    const newOrder = result.rows[0];

    // If associated_container is being assigned
    const associatedContainer = updates.associated_container;
    if (associatedContainer) {
      const containerCheck = await client.query(
        'SELECT cid FROM container_master WHERE container_number = $1 AND status = 1',
        [associatedContainer]
      );
      if (containerCheck.rowCount === 0) {
        await client.query('ROLLBACK');
        return res.status(400).json({ error: 'Associated container not found' });
      }
      const cid = containerCheck.rows[0].cid;
      const availability = getContainerAvailability(updates.status || 'In Transit');
      await client.query(
        'INSERT INTO container_status (cid, availability, status_notes, created_by) VALUES ($1, $2, $3, $4)',
        [cid, availability, `Assigned to order ${newOrder.id} (${updates.status || 'In Transit'})`, created_by]
      );
    }

    await client.query('COMMIT');
    console.log("Created new order:", { id: newOrder.id });
    res.status(201).json(newOrder);
  } catch (error) {
    if (client) {
      await client.query('ROLLBACK');
    }
    console.error('Error creating order:', error);
    // Enhanced error handling
    if (error.code === '23505') {
      return res.status(409).json({ error: 'Booking reference already exists' });
    }
    if (error.code === '23514') {
      return res.status(400).json({ error: 'Invalid value for constrained field' });
    }
    if (error.code === '22007' || error.code === '22008') {
      return res.status(400).json({ error: 'Invalid date format' });
    }
    if (error.code === '42P01') { // Column does not exist
      return res.status(500).json({ error: 'Database schema missing columns. Run migrations for new fields.' });
    }
    res.status(500).json({ error: 'Failed to create order', details: error.message });
  } finally {
    if (client) {
      client.release();
    }
  }
}

// Update an existing order (handles multipart/form-data for files; refined validation)
export async function updateOrder(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');

    const { id } = req.params;
    const updates = req.body;
    const files = req.files || {}; // Assuming multer .fields() or .any()
    const created_by = updates.created_by || 'system';

    // Debug log
    console.log('Order update body (key fields):', { booking_ref: updates.booking_ref, status: updates.status, eta: updates.eta, etd: updates.etd, shipping_line: updates.shipping_line });
    console.log('Files received:', Object.keys(files));

    // Fetch current order
    const currentResult = await client.query('SELECT * FROM orders WHERE id = $1', [id]);
    if (currentResult.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Order not found' });
    }
    const currentOrder = currentResult.rows[0];

    // Parse existing attachments and gatepass if JSON strings
    let currentAttachments = [];
    if (currentOrder.attachments) {
      try {
        currentAttachments = typeof currentOrder.attachments === 'string' ? JSON.parse(currentOrder.attachments) : currentOrder.attachments;
      } catch (e) {
        currentAttachments = [];
      }
    }
    let currentGatepass = [];
    if (currentOrder.gatepass) {
      try {
        currentGatepass = typeof currentOrder.gatepass === 'string' ? JSON.parse(currentOrder.gatepass) : currentOrder.gatepass;
      } catch (e) {
        currentGatepass = [];
      }
    }

    // Handle attachments
    let newAttachments = currentAttachments;
    let existingAttachmentsFromForm = [];
    if (updates.attachments_existing) {
      try {
        existingAttachmentsFromForm = JSON.parse(updates.attachments_existing);
      } catch (e) {
        console.warn('Failed to parse attachments_existing:', e.message);
      }
    }
    if (existingAttachmentsFromForm.length > 0) {
      newAttachments = existingAttachmentsFromForm; // Use form's existing if provided (e.g., text-only update)
    }
    if (files.attachments && files.attachments.length > 0) {
      const uploadedPaths = await uploadFiles(files.attachments, 'attachments'); // Returns relative paths
      newAttachments = [...newAttachments, ...uploadedPaths];
    }
    const attachmentsJson = JSON.stringify(newAttachments);

    // Handle gatepass (similarly to attachments)
    let newGatepass = currentGatepass;
    let existingGatepassFromForm = [];
    if (updates.gatepass_existing) {
      try {
        existingGatepassFromForm = JSON.parse(updates.gatepass_existing);
      } catch (e) {
        console.warn('Failed to parse gatepass_existing:', e.message);
      }
    }
    if (existingGatepassFromForm.length > 0) {
      newGatepass = existingGatepassFromForm; // Use form's existing if provided
    }
    if (files.gatepass && files.gatepass.length > 0) {
      const uploadedPaths = await uploadFiles(files.gatepass, 'gatepass'); // Returns relative paths
      newGatepass = [...newGatepass, ...uploadedPaths];
    }
    const gatepassJson = JSON.stringify(newGatepass);

    // Allowed update fields (now includes attachments, gatepass)
    const allowedUpdateFields = [
      'booking_ref', 'status', 'eta', 'etd', 'place_of_loading', 'final_destination',
      'place_of_delivery', 'sender_name', 'receiver_name', 'shipping_line',
      'associated_container', 'consignment_remarks', 'rgl_booking_number',
      'order_remarks', 'consignment_number', 'consignment_vessel',
      'consignment_voyage', 'sender_contact', 'sender_address', 'sender_email',
      'receiver_contact', 'receiver_address', 'receiver_email', 'driver_name',
      'driver_contact', 'driver_nic', 'driver_pickup_location', 'truck_number',
      'third_party_transport', 'category', 'subcategory', 'type', 'delivery_address',
      'pickup_location', 'weight', 'drop_method', 'drop_off_cnic', 'drop_off_mobile',
      'plate_no', 'drop_date', 'collection_method', 'full_partial', 'qty_delivered',
      'client_receiver_name', 'client_receiver_id', 'client_receiver_mobile',
      'delivery_date', 'attachments', 'gatepass'
    ];

    // Date keys to exclude from text fields
    const dateKeys = ['eta', 'etd', 'drop_date', 'delivery_date'];

    // Numeric fields that should be null if empty
    const numericFields = ['weight', 'qty_delivered'];

    // Validation (only for provided fields)
    const updatedFields = {
      bookingRef: updates.booking_ref,
      status: updates.status,
      eta: updates.eta,
      etd: updates.etd,
      placeOfLoading: updates.place_of_loading,
      finalDestination: updates.final_destination,
      placeOfDelivery: updates.place_of_delivery,
      senderName: updates.sender_name,
      receiverName: updates.receiver_name,
      shippingLine: updates.shipping_line,
      rglBookingNumber: updates.rgl_booking_number,
      consignmentRemarks: updates.consignment_remarks,
      orderRemarks: updates.order_remarks,
      consignmentNumber: updates.consignment_number,
      consignmentVessel: updates.consignment_vessel,
      consignmentVoyage: updates.consignment_voyage,
      senderContact: updates.sender_contact,
      senderAddress: updates.sender_address,
      senderEmail: updates.sender_email,
      receiverContact: updates.receiver_contact,
      receiverAddress: updates.receiver_address,
      receiverEmail: updates.receiver_email,
      driverName: updates.driver_name,
      driverContact: updates.driver_contact,
      driverNic: updates.driver_nic,
      driverPickupLocation: updates.driver_pickup_location,
      truckNumber: updates.truck_number,
      thirdPartyTransport: updates.third_party_transport,
      category: updates.category,
      subcategory: updates.subcategory,
      type: updates.type,
      deliveryAddress: updates.delivery_address,
      pickupLocation: updates.pickup_location,
      weight: updates.weight,
      dropMethod: updates.drop_method,
      dropOffCnic: updates.drop_off_cnic,
      dropOffMobile: updates.drop_off_mobile,
      plateNo: updates.plate_no,
      dropDate: updates.drop_date,
      collectionMethod: updates.collection_method,
      fullPartial: updates.full_partial,
      qtyDelivered: updates.qty_delivered,
      clientReceiverName: updates.client_receiver_name,
      clientReceiverId: updates.client_receiver_id,
      clientReceiverMobile: updates.client_receiver_mobile,
      deliveryDate: updates.delivery_date,
    };
    const updateErrors = [];
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    const mobileRegex = /^\d{10,15}$/;

    // Helper to get actual field name (snake_case)
    const getActualField = (camelField) => {
      switch (camelField) {
        case 'bookingRef': return 'booking_ref';
        case 'rglBookingNumber': return 'rgl_booking_number';
        case 'placeOfLoading': return 'place_of_loading';
        case 'finalDestination': return 'final_destination';
        case 'placeOfDelivery': return 'place_of_delivery';
        case 'senderName': return 'sender_name';
        case 'receiverName': return 'receiver_name';
        case 'shippingLine': return 'shipping_line';
        case 'consignmentRemarks': return 'consignment_remarks';
        case 'orderRemarks': return 'order_remarks';
        case 'consignmentNumber': return 'consignment_number';
        case 'consignmentVessel': return 'consignment_vessel';
        case 'consignmentVoyage': return 'consignment_voyage';
        case 'senderContact': return 'sender_contact';
        case 'senderAddress': return 'sender_address';
        case 'senderEmail': return 'sender_email';
        case 'receiverContact': return 'receiver_contact';
        case 'receiverAddress': return 'receiver_address';
        case 'receiverEmail': return 'receiver_email';
        case 'driverName': return 'driver_name';
        case 'driverContact': return 'driver_contact';
        case 'driverNic': return 'driver_nic';
        case 'driverPickupLocation': return 'driver_pickup_location';
        case 'truckNumber': return 'truck_number';
        case 'thirdPartyTransport': return 'third_party_transport';
        case 'deliveryAddress': return 'delivery_address';
        case 'pickupLocation': return 'pickup_location';
        case 'dropMethod': return 'drop_method';
        case 'dropOffCnic': return 'drop_off_cnic';
        case 'dropOffMobile': return 'drop_off_mobile';
        case 'plateNo': return 'plate_no';
        case 'dropDate': return 'drop_date';
        case 'collectionMethod': return 'collection_method';
        case 'fullPartial': return 'full_partial';
        case 'qtyDelivered': return 'qty_delivered';
        case 'clientReceiverName': return 'client_receiver_name';
        case 'clientReceiverId': return 'client_receiver_id';
        case 'clientReceiverMobile': return 'client_receiver_mobile';
        case 'deliveryDate': return 'delivery_date';
        default: return camelField.toLowerCase().replace(/([A-Z])/g, '_$1').toLowerCase();
      }
    };

    // Required fields validation using effective values
    const requiredFields = [
      'bookingRef',
      'rglBookingNumber',
      'senderName',
      'receiverName',
      'placeOfLoading',
      'finalDestination',
      'category',
      'subcategory',
      'type',
      'weight'
    ];

    requiredFields.forEach(camelField => {
      const actualField = getActualField(camelField);
      const effectiveValue = updates[actualField] !== undefined ? updates[actualField] : currentOrder[actualField];
      if (camelField === 'weight') {
        const numValue = parseFloat(effectiveValue);
        if (isNaN(numValue) || numValue <= 0) {
          updateErrors.push(`${actualField} must be a positive number`);
        }
      } else if (!effectiveValue || (typeof effectiveValue === 'string' && !effectiveValue.trim())) {
        updateErrors.push(`${actualField} is required`);
      }
    });

    // Effective values for conditionals
    const effectiveFinalDestination = updates.final_destination !== undefined ? updates.final_destination : currentOrder.final_destination;
    const showInbound = !!effectiveFinalDestination && effectiveFinalDestination.includes('Karachi');
    const effectivePlaceOfLoading = updates.place_of_loading !== undefined ? updates.place_of_loading : currentOrder.place_of_loading;
    const showOutbound = !!effectivePlaceOfLoading && effectivePlaceOfLoading.includes('Dubai');

    const effectiveDropMethod = updates.drop_method !== undefined ? updates.drop_method : currentOrder.drop_method;
    if (showInbound && effectiveDropMethod === 'Drop-Off') {
      const effectiveDropOffCnic = updates.drop_off_cnic !== undefined ? updates.drop_off_cnic : currentOrder.drop_off_cnic;
      if (!effectiveDropOffCnic || (typeof effectiveDropOffCnic === 'string' && !effectiveDropOffCnic.trim())) {
        updateErrors.push('drop_off_cnic required for Drop-Off');
      }
      const effectiveDropOffMobile = updates.drop_off_mobile !== undefined ? updates.drop_off_mobile : currentOrder.drop_off_mobile;
      if (!effectiveDropOffMobile || (typeof effectiveDropOffMobile === 'string' && !effectiveDropOffMobile.trim())) {
        updateErrors.push('drop_off_mobile required for Drop-Off');
      }
    }
    if (showInbound) {
      const effectiveDropDate = updates.drop_date !== undefined ? updates.drop_date : currentOrder.drop_date;
      if (!effectiveDropDate || (typeof effectiveDropDate === 'string' && !effectiveDropDate.trim())) {
        updateErrors.push('drop_date required');
      }
    }

    if (showOutbound) {
      const effectiveDeliveryDate = updates.delivery_date !== undefined ? updates.delivery_date : currentOrder.delivery_date;
      if (!effectiveDeliveryDate || (typeof effectiveDeliveryDate === 'string' && !effectiveDeliveryDate.trim())) {
        updateErrors.push('delivery_date required');
      }
    }
    const effectiveFullPartial = updates.full_partial !== undefined ? updates.full_partial : currentOrder.full_partial;
    if (showOutbound && effectiveFullPartial === 'Partial') {
      const effectiveQtyDelivered = updates.qty_delivered !== undefined ? updates.qty_delivered : currentOrder.qty_delivered;
      if (!effectiveQtyDelivered || (typeof effectiveQtyDelivered === 'string' && !effectiveQtyDelivered.trim())) {
        updateErrors.push('qty_delivered required for Partial delivery');
      }
    }
    const effectiveCollectionMethod = updates.collection_method !== undefined ? updates.collection_method : currentOrder.collection_method;
    if (showOutbound && effectiveCollectionMethod === 'Collected by Client') {
      const effectiveClientReceiverName = updates.client_receiver_name !== undefined ? updates.client_receiver_name : currentOrder.client_receiver_name;
      if (!effectiveClientReceiverName || (typeof effectiveClientReceiverName === 'string' && !effectiveClientReceiverName.trim())) {
        updateErrors.push('client_receiver_name required for Client Collection');
      }
      const effectiveClientReceiverId = updates.client_receiver_id !== undefined ? updates.client_receiver_id : currentOrder.client_receiver_id;
      if (!effectiveClientReceiverId || (typeof effectiveClientReceiverId === 'string' && !effectiveClientReceiverId.trim())) {
        updateErrors.push('client_receiver_id required for Client Collection');
      }
      const effectiveClientReceiverMobile = updates.client_receiver_mobile !== undefined ? updates.client_receiver_mobile : currentOrder.client_receiver_mobile;
      if (!effectiveClientReceiverMobile || (typeof effectiveClientReceiverMobile === 'string' && !effectiveClientReceiverMobile.trim())) {
        updateErrors.push('client_receiver_mobile required for Client Collection');
      }
    }

    // Format validations (only if provided)
    for (const [camelField, value] of Object.entries(updatedFields)) {
      if (value !== undefined) {
        const actualField = getActualField(camelField);
        // Skip format check if empty string (allow updating to empty for optionals)
        const trimmedValue = typeof value === 'string' ? value.trim() : value;
        if (trimmedValue !== '' && trimmedValue !== null && trimmedValue !== undefined) {
          if (['eta', 'etd', 'dropDate', 'deliveryDate'].includes(camelField) && !isValidDate(value)) {
            updateErrors.push(`${actualField} invalid date format (use YYYY-MM-DD)`);
          } else if (camelField === 'weight' && (isNaN(value) || parseFloat(value) <= 0)) {
            updateErrors.push(`${actualField} must be a positive number`);
          } else if (['senderEmail', 'receiverEmail'].includes(camelField) && !emailRegex.test(value)) {
            updateErrors.push(`${actualField} invalid email format`);
          } else if (['dropOffMobile', 'clientReceiverMobile'].includes(camelField) && !mobileRegex.test(value.replace(/\D/g, ''))) {
            updateErrors.push(`${actualField} invalid mobile number (10-15 digits expected)`);
          }
          // Add more specific validations as needed (e.g., enums for status, drop_method, etc.)
        }
      }
    }

    if (updateErrors.length > 0) {
      console.warn('Update validation failed:', updateErrors);
      await client.query('ROLLBACK');
      return res.status(400).json({ 
        error: 'Invalid update fields',
        details: updateErrors.join('; ')
      });
    }    // Normalize dates
    const normEta = updates.eta ? normalizeDate(updates.eta) : currentOrder.eta;
    const normEtd = updates.etd ? normalizeDate(updates.etd) : currentOrder.etd;
    const normDropDate = updates.drop_date ? normalizeDate(updates.drop_date) : currentOrder.drop_date;
    const normDeliveryDate = updates.delivery_date ? normalizeDate(updates.delivery_date) : currentOrder.delivery_date;

    if ((updates.eta && !normEta) || (updates.etd && !normEtd) || (updates.drop_date && !normDropDate) || (updates.delivery_date && !normDeliveryDate)) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'Invalid date format. Use YYYY-MM-DD.' });
    }

    // If status is being updated, handle container status change (insert new history row)
    if (updates.status && updates.status !== currentOrder.status) {
      if (currentOrder.associated_container) {
        const containerCheck = await client.query(
          'SELECT cid FROM container_master WHERE container_number = $1 AND status = 1',
          [currentOrder.associated_container]
        );
        if (containerCheck.rowCount > 0) {
          const cid = containerCheck.rows[0].cid;
          const newAvailability = getContainerAvailability(updates.status);
          await client.query(
            'INSERT INTO container_status (cid, availability, status_notes, created_by) VALUES ($1, $2, $3, $4)',
            [cid, newAvailability, `Updated via order ${id} status to ${updates.status}`, created_by]
          );
        }
      }
    }

    // If associated_container is being updated (assigned/unassigned)
    if (updates.associated_container !== undefined && updates.associated_container !== currentOrder.associated_container) {
      // Unassign from old container if exists (insert 'Available' history)
      if (currentOrder.associated_container) {
        const oldContainerCheck = await client.query(
          'SELECT cid FROM container_master WHERE container_number = $1 AND status = 1',
          [currentOrder.associated_container]
        );
        if (oldContainerCheck.rowCount > 0) {
          const oldCid = oldContainerCheck.rows[0].cid;
          await client.query(
            'INSERT INTO container_status (cid, availability, status_notes, created_by) VALUES ($1, $2, $3, $4)',
            [oldCid, 'Available', `Unassigned from order ${id}`, created_by]
          );
        }
      }

      // Assign to new container if provided (insert new availability history)
      if (updates.associated_container) {
        const newContainerCheck = await client.query(
          'SELECT cid FROM container_master WHERE container_number = $1 AND status = 1',
          [updates.associated_container]
        );
        if (newContainerCheck.rowCount === 0) {
          await client.query('ROLLBACK');
          return res.status(400).json({ error: 'New associated container not found' });
        }
        const newCid = newContainerCheck.rows[0].cid;
        const availability = getContainerAvailability(updates.status || currentOrder.status);
        await client.query(
          'INSERT INTO container_status (cid, availability, status_notes, created_by) VALUES ($1, $2, $3, $4)',
          [newCid, availability, `Assigned to order ${id} (${updates.status || currentOrder.status})`, created_by]
        );
      }
    }

    // Prepare updates including files
    const textUpdateFields = Object.keys(updates).filter(key => key !== 'id' && allowedUpdateFields.includes(key) && !key.endsWith('_existing') && !dateKeys.includes(key));
    const allUpdates = {
      ...updates,
      eta: normEta,
      etd: normEtd,
      drop_date: normDropDate,
      delivery_date: normDeliveryDate,
      attachments: attachmentsJson,
      gatepass: gatepassJson
    };

    if (textUpdateFields.length > 0 || true) { // Always update files if changed
      const setClauseParts = [];
      const values = [];
      let paramIndex = 1;

      // Text fields (excluding dates)
      textUpdateFields.forEach(key => {
        let val = updates[key];
        if (numericFields.includes(key) && (val === '' || val === null || val === undefined)) {
          val = null;
        }
        setClauseParts.push(`${key} = $${paramIndex}`);
        values.push(val);  // Use processed value
        paramIndex++;
      });

      // Always include attachments and gatepass if they differ
      if (JSON.stringify(currentOrder.attachments) !== attachmentsJson) {
        setClauseParts.push(`attachments = $${paramIndex}`);
        values.push(attachmentsJson);
        paramIndex++;
      }
      if (JSON.stringify(currentOrder.gatepass) !== gatepassJson) {
        setClauseParts.push(`gatepass = $${paramIndex}`);
        values.push(gatepassJson);
        paramIndex++;
      }

      // Dates if provided
      if (updates.eta !== undefined) {
        setClauseParts.push(`eta = $${paramIndex}`);
        values.push(normEta);
        paramIndex++;
      }
      if (updates.etd !== undefined) {
        setClauseParts.push(`etd = $${paramIndex}`);
        values.push(normEtd);
        paramIndex++;
      }
      if (updates.drop_date !== undefined) {
        setClauseParts.push(`drop_date = $${paramIndex}`);
        values.push(normDropDate);
        paramIndex++;
      }
      if (updates.delivery_date !== undefined) {
        setClauseParts.push(`delivery_date = $${paramIndex}`);
        values.push(normDeliveryDate);
        paramIndex++;
      }

      const setClause = setClauseParts.join(', ');
      values.push(id); // WHERE id = $last
      const updateQuery = `UPDATE orders SET ${setClause}, updated_at = CURRENT_TIMESTAMP WHERE id = $${paramIndex} RETURNING *`;
      const updateResult = await client.query(updateQuery, values);
      await client.query('COMMIT');
      console.log("Updated order:", id);
      res.json(updateResult.rows[0]);
    } else {
      await client.query('COMMIT');
      res.json(currentOrder);
    }
  } catch (error) {
    if (client) {
      await client.query('ROLLBACK');
    }
    console.error('Error updating order:', error);
    // Enhanced error handling
    if (error.code === '23505') {
      return res.status(409).json({ error: 'Booking reference already exists' });
    }
    if (error.code === '23514') {
      return res.status(400).json({ error: 'Invalid value for constrained field' });
    }
    if (error.code === '22007' || error.code === '22008') {
      return res.status(400).json({ error: 'Invalid date format' });
    }
    if (error.code === '42P01') { // Column does not exist
      return res.status(500).json({ error: 'Database schema missing columns. Run migrations for new fields.' });
    }
    res.status(500).json({ error: 'Failed to update order', details: error.message });
  } finally {
    if (client) {
      client.release();
    }
  }
}

// Helper function (Multer already saves files; just return relative paths)
async function uploadFiles(files, type) {
  console.log('Controller execution', files, type);
  const paths = [];
  for (const file of files) {
    const relativePath = `/uploads/${type}/${file.filename}`;
    paths.push(relativePath);
  }
  return paths;
}

// getOrders, getOrderById, getOrderStatuses, getOrderUsageHistory, cancelOrder (unchanged from previous)
export async function getOrders(req, res) {
  try {
    const { page = 1, limit = 50, ...filters } = req.query;

    // Build dynamic WHERE clause for base filters (non-computed)
    let baseWhereClause = "WHERE o.status != 'Cancelled' AND o.status IS NOT NULL";  // Filter active orders
    const baseParams = [];
    let paramIndex = 1;

    // Add filters for order fields (adjust field names to match schema)
    if (filters.booking_ref) {
      baseWhereClause += ` AND o.booking_ref ILIKE $${paramIndex}`;
      baseParams.push(`%${filters.booking_ref}%`);
      paramIndex++;
    }
    if (filters.status) {
      baseWhereClause += ` AND o.status = $${paramIndex}`;
      baseParams.push(filters.status);
      paramIndex++;
    }
    if (filters.customer) {
      baseWhereClause += ` AND (o.sender_name ILIKE $${paramIndex} OR o.receiver_name ILIKE $${paramIndex})`;  // Search sender/receiver
      baseParams.push(`%${filters.customer}%`);
      paramIndex++;
    }
    // New filters for additional fields
    if (filters.category) {
      baseWhereClause += ` AND o.category ILIKE $${paramIndex}`;
      baseParams.push(`%${filters.category}%`);
      paramIndex++;
    }
    if (filters.sender_name) {
      baseWhereClause += ` AND o.sender_name ILIKE $${paramIndex}`;
      baseParams.push(`%${filters.sender_name}%`);
      paramIndex++;
    }
    if (filters.receiver_name) {
      baseWhereClause += ` AND o.receiver_name ILIKE $${paramIndex}`;
      baseParams.push(`%${filters.receiver_name}%`);
      paramIndex++;
    }
    // Example date filter: if (filters.eta_from && filters.eta_to) { ... }

    // If filtering on container-related fields, we'll handle via join
    const containerFilter = filters.container_number ? filters.container_number : null;

    // Compute placeholder positions for LIMIT/OFFSET *before* building query
    let containerParamPosition = containerFilter ? paramIndex : 0;  // 0 if no container filter
    const limitPosition = paramIndex + (containerFilter ? 1 : 0);   // Position for LIMIT (after container if present)
    const offsetPosition = limitPosition + 1;                       // Position for OFFSET

    // Main query with CTE for computed container_derived_status (aligned with container CTE)
    const query = `
      WITH computed_orders AS (
        SELECT 
          o.*,  -- All order fields (e.g., id, booking_ref, status, category, subcategory, type, delivery_address, pickup_location, weight, drop_method, drop_off_cnic, drop_off_mobile, plate_no, drop_date, collection_method, full_partial, qty_delivered, client_receiver_name, client_receiver_id, client_receiver_mobile, delivery_date, etc.)
          -- Container details from joined table
          cm.container_number,
          cm.container_size,
          cm.container_type,
          -- Computed derived status for the associated container (manual override or dynamic logic)
          COALESCE(cm.manual_derived_status, 
            -- Dynamic computation based on hire dates and latest availability
            CASE 
              -- Returned if hire ended before today and cleared
              WHEN chd.hire_end_date < CURRENT_DATE AND cs.availability = 'Cleared' THEN 'Returned'
              -- Hired if start date set but no end date
              WHEN chd.hire_end_date IS NULL AND chd.hire_start_date IS NOT NULL THEN 'Hired'
              -- Occupied if hire end date in future
              WHEN chd.hire_end_date > CURRENT_DATE THEN 'Occupied'
              -- Direct availability mappings for transit/loaded states
              WHEN cs.availability IN ('In Transit', 'Loaded', 'Assigned to Job') THEN cs.availability
              -- Specific availability states
              WHEN cs.availability = 'Arrived' THEN 'Arrived'
              WHEN cs.availability = 'De-Linked' THEN 'De-Linked'
              WHEN cs.availability = 'Under Repair' THEN 'Under Repair'
              WHEN cs.availability = 'Returned' THEN 'Returned'
              -- Default fallback
              ELSE 'Available'
            END
          ) AS container_derived_status  -- Alias for the computed container status
        -- From main orders table
        FROM orders o  -- Main table: core order data
        -- Join to container details via container number
        LEFT JOIN container_master cm ON o.associated_container = cm.container_number
        -- Lateral join for latest container status (most recent record per container)
        LEFT JOIN LATERAL (
          SELECT location, availability  -- Get location and availability
          FROM container_status css      -- From status history
          WHERE css.cid = cm.cid         -- Match container ID
          ORDER BY css.sid DESC NULLS LAST  -- Latest by status ID (descending, nulls last)
          LIMIT 1                        -- Only one row per container
        ) cs ON true                       -- Lateral join condition (always true for each row)
        -- Lateral join for active/latest hire details (avoids duplicates)
        LEFT JOIN LATERAL (
          SELECT hire_start_date, hire_end_date, hired_by
          FROM container_hire_details chd_inner
          WHERE chd_inner.cid = cm.cid
            AND (chd_inner.hire_end_date IS NULL OR chd_inner.hire_end_date >= CURRENT_DATE)
          ORDER BY chd_inner.hire_start_date DESC NULLS LAST
          LIMIT 1
        ) chd ON true
        ${baseWhereClause}
      )
      SELECT * FROM computed_orders
      ${containerFilter ? `WHERE container_number ILIKE $${containerParamPosition}` : ''}
      -- Order by creation date (newest first) - FIXED: No alias "o" in outer query
      ORDER BY created_at DESC
      -- Pagination (Use pre-computed positions; no spaces or extra $)
      LIMIT $${limitPosition} OFFSET $${offsetPosition}
    `;

    // Build full params for main query
    let fullParams = [...baseParams];
    if (containerFilter) {
      fullParams.push(`%${containerFilter}%`);
    }
    fullParams.push(parseInt(limit));
    fullParams.push((parseInt(page) - 1) * parseInt(limit));

    // Debug the computed positions and full query
    console.log('Computed Positions - Limit:', limitPosition, 'Offset:', offsetPosition);
    console.log('Full Query:', query); // Debug log
    console.log('Full Params:', fullParams); // Debug log

    const result = await pool.query(query, fullParams);

    // Count query (aligned with container count: use CTE)
    let countParamIndex = 1;
    let countBaseParams = [];
    let countWhereClause = "WHERE o.status != 'Cancelled' AND o.status IS NOT NULL";
    if (filters.booking_ref) {
      countWhereClause += ` AND o.booking_ref ILIKE $${countParamIndex}`;
      countBaseParams.push(`%${filters.booking_ref}%`);
      countParamIndex++;
    }
    if (filters.status) {
      countWhereClause += ` AND o.status = $${countParamIndex}`;
      countBaseParams.push(filters.status);
      countParamIndex++;
    }
    if (filters.customer) {
      countWhereClause += ` AND (o.sender_name ILIKE $${countParamIndex} OR o.receiver_name ILIKE $${countParamIndex})`;
      countBaseParams.push(`%${filters.customer}%`);
      countParamIndex++;
    }
    if (filters.category) {
      countWhereClause += ` AND o.category ILIKE $${countParamIndex}`;
      countBaseParams.push(`%${filters.category}%`);
      countParamIndex++;
    }
    if (filters.sender_name) {
      countWhereClause += ` AND o.sender_name ILIKE $${countParamIndex}`;
      countBaseParams.push(`%${filters.sender_name}%`);
      countParamIndex++;
    }
    if (filters.receiver_name) {
      countWhereClause += ` AND o.receiver_name ILIKE $${countParamIndex}`;
      countBaseParams.push(`%${filters.receiver_name}%`);
      countParamIndex++;
    }
    const countContainerPosition = containerFilter ? countParamIndex : 0;
    let countWhereAdd = containerFilter ? ` AND cm.container_number ILIKE $${countContainerPosition}` : '';
    let countParams = [...countBaseParams];
    if (containerFilter) {
      countParams.push(`%${containerFilter}%`);
    }

    const countQuery = `
      -- Count total matching orders for pagination
      SELECT COUNT(*) as total
      -- From main orders table with container join
      FROM orders o
      LEFT JOIN container_master cm ON o.associated_container = cm.container_number
      -- Apply same filters as main query
      ${countWhereClause}
      ${countWhereAdd}
    `;
    const countResult = await pool.query(countQuery, countParams);
    const total = parseInt(countResult.rows[0]?.total || 0);

    // Parse attachments and gatepass for easier use (handles empty string)
    const dataWithParsedAttachments = result.rows.map(row => {
      let parsedAttachments = [];
      if (row.attachments && typeof row.attachments === 'string' && row.attachments.trim() !== '') {
        try {
          parsedAttachments = JSON.parse(row.attachments);
        } catch (parseErr) {
          console.warn('Invalid JSON in attachments for order', row.id, ':', parseErr.message);
          parsedAttachments = [];  // Fallback to empty array
        }
      }
      let parsedGatepass = [];
      if (row.gatepass && typeof row.gatepass === 'string' && row.gatepass.trim() !== '') {
        try {
          parsedGatepass = JSON.parse(row.gatepass);
        } catch (parseErr) {
          console.warn('Invalid JSON in gatepass for order', row.id, ':', parseErr.message);
          parsedGatepass = [];  // Fallback to empty array
        }
      }
      return {
        ...row,
        attachments: Array.isArray(parsedAttachments) ? parsedAttachments : [],
        gatepass: Array.isArray(parsedGatepass) ? parsedGatepass : [],
        color: getOrderStatusColor(row.status)
      };
    });

    console.log(`Fetched orders: ${dataWithParsedAttachments.length} Total: ${total} Filters:`, req.query);
    res.json({
      data: dataWithParsedAttachments,
      total,
      page: parseInt(page),
      limit: parseInt(limit)
    });
  } catch (err) {
    console.error("Error fetching orders:", err.message, "Query params:", req.query);
    if (err.code === '23514') {
      return res.status(400).json({ error: 'Invalid value for constrained field' });
    }
    res.status(500).json({ error: 'Failed to fetch orders', details: err.message });
  }
}
export async function getOrderById(req, res) {
  try {
    const { id } = req.params;
    const { includeContainer = 'true' } = req.query; // Default to true for consistency with list view

    let query = ` 
      SELECT o.*,
        cm.container_number as container_number_full,
        cm.container_size,
        cm.container_type,
        cm.owner_type,
        cs.location as container_location,
        cs.availability as container_availability
    `;
    let fromClause = `
      FROM orders o
      LEFT JOIN container_master cm ON o.associated_container = cm.container_number
      LEFT JOIN LATERAL (
        SELECT location, availability
        FROM container_status css
        WHERE css.cid = cm.cid
        ORDER BY css.sid DESC NULLS LAST
        LIMIT 1
      ) cs ON true
    `;
    let whereClause = 'o.id = $1';

    if (includeContainer === 'true') {
      query += `,
        COALESCE(cm.manual_derived_status, 
          CASE 
            WHEN cs.availability = 'Cleared' THEN 'Cleared'
            WHEN chd.hire_end_date < CURRENT_DATE AND cs.availability = 'Cleared' THEN 'Returned'
            WHEN chd.hire_end_date IS NULL AND chd.hire_start_date IS NOT NULL THEN 'Hired'
            WHEN chd.hire_end_date > CURRENT_DATE THEN 'Occupied'
            WHEN cs.availability IN ('In Transit', 'Loaded', 'Assigned to Job') THEN cs.availability
            WHEN cs.availability = 'Arrived' THEN 'Arrived'
            WHEN cs.availability = 'De-Linked' THEN 'De-Linked'
            WHEN cs.availability = 'Under Repair' THEN 'Under Repair'
            WHEN cs.availability = 'Returned' THEN 'Returned'
            ELSE 'Available'
          END
        ) as container_derived_status,
        cpd.manufacture_date as container_manufacture_date,
        cpd.purchase_date as container_purchase_date,
        cpd.purchase_price as container_purchase_price,
        cpd.owned_by as container_owned_by,
        chd.hire_start_date as container_hire_start_date,
        chd.hire_end_date as container_hire_end_date,
        chd.hired_by as container_hired_by
      `;
      fromClause += `
        LEFT JOIN container_purchase_details cpd ON cm.cid = cpd.cid
        LEFT JOIN LATERAL (
          SELECT hire_start_date, hire_end_date, hired_by
          FROM container_hire_details chd_inner
          WHERE chd_inner.cid = cm.cid
            AND (chd_inner.hire_end_date IS NULL OR chd_inner.hire_end_date >= CURRENT_DATE)
          ORDER BY chd_inner.hire_start_date DESC NULLS LAST
          LIMIT 1
        ) chd ON true
      `;
    }

    query += ` ${fromClause} WHERE ${whereClause} ORDER BY o.created_at DESC`;

    const result = await pool.query(query, [id]);
    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Order not found' });
    }

    // Parse attachments and gatepass for easier use (handles arrays, strings, null, or empty)
    const row = result.rows[0];
    let parsedAttachments = [];
    const attachmentsValue = row.attachments;
    if (attachmentsValue) {
      if (Array.isArray(attachmentsValue)) {
        parsedAttachments = attachmentsValue;  // Already parsed by pg (JSONB)
      } else if (typeof attachmentsValue === 'string' && attachmentsValue.trim() !== '') {
        try {
          parsedAttachments = JSON.parse(attachmentsValue);
        } catch (parseErr) {
          console.warn('Invalid JSON in attachments for order', row.id, ':', parseErr.message);
          parsedAttachments = [];  // Fallback to empty array
        }
      }
      // If it's an object or other type, log and fallback (edge case)
      else if (typeof attachmentsValue === 'object') {
        console.warn('Unexpected object type for attachments in order', row.id);
        parsedAttachments = [];  // Or convert if needed
      }
    }

    let parsedGatepass = [];
    const gatepassValue = row.gatepass;
    if (gatepassValue) {
      if (Array.isArray(gatepassValue)) {
        parsedGatepass = gatepassValue;  // Already parsed by pg (JSONB)
      } else if (typeof gatepassValue === 'string' && gatepassValue.trim() !== '') {
        try {
          parsedGatepass = JSON.parse(gatepassValue);
        } catch (parseErr) {
          console.warn('Invalid JSON in gatepass for order', row.id, ':', parseErr.message);
          parsedGatepass = [];  // Fallback to empty array
        }
      }
      // If it's an object or other type, log and fallback (edge case)
      else if (typeof gatepassValue === 'object') {
        console.warn('Unexpected object type for gatepass in order', row.id);
        parsedGatepass = [];  // Or convert if needed
      }
    }
    const orderData = {
      ...row,
      attachments: Array.isArray(parsedAttachments) ? parsedAttachments : [],
      gatepass: Array.isArray(parsedGatepass) ? parsedGatepass : [],
      color: getOrderStatusColor(row.status)
    };

    console.log(`Fetched order: ${orderData.booking_ref || orderData.id}`);

    res.json(orderData);
  } catch (err) {
    console.error("Error fetching order by ID:", err.message, "Params:", req.params);
    if (err.code === '23514') {
      return res.status(400).json({ error: 'Invalid value for constrained field' });
    }
    res.status(500).json({ error: 'Failed to fetch order', details: err.message });
  }
}


export async function getOrderStatuses(req, res) {
  try {
    // Hardcode full list for consistency (or query distinct if preferred)
    const statuses = [
      { value: 'Created', label: 'Created', color: getOrderStatusColor('Created') },
      { value: 'In Transit', label: 'In Transit', color: getOrderStatusColor('In Transit') },
      { value: 'Delivered', label: 'Delivered', color: getOrderStatusColor('Delivered') },
      { value: 'Cancelled', label: 'Cancelled', color: getOrderStatusColor('Cancelled') }
    ];
    res.json(statuses);
  } catch (err) {
    console.error("Error fetching order statuses:", err);
    res.status(500).json({ error: 'Failed to fetch order statuses' });
  }
}

export async function getOrderUsageHistory(req, res) {
  try {
    const { id } = req.params;
    const query = `
      SELECT 
        o.id, o.booking_ref as job_no, o.status, o.created_at as start_date, o.updated_at as end_date,
        o.place_of_loading as pol, o.final_destination as pod, o.consignment_remarks as remarks,
        cm.container_number, cm.owner_type,
        cs.availability as status,
        cpd.owned_by, chd.hired_by
      FROM orders o
      LEFT JOIN container_master cm ON o.associated_container = cm.container_number
      LEFT JOIN LATERAL (
        SELECT availability
        FROM container_status css
        WHERE css.cid = cm.cid
        ORDER BY css.sid DESC NULLS LAST
        LIMIT 1
      ) cs ON true
      LEFT JOIN container_purchase_details cpd ON cm.cid = cpd.cid
      LEFT JOIN container_hire_details chd ON cm.cid = chd.cid
      WHERE o.id = $1
      ORDER BY o.created_at DESC
    `;
    const result = await pool.query(query, [id]);
    const history = result.rows;

    // Format for frontend
    const formattedHistory = history.map(row => ({
      jobNo: row.job_no,
      pol: row.pol || 'N/A',
      pod: row.pod || 'N/A',
      startDate: row.start_date ? row.start_date.toISOString().split('T')[0] : null,
      endDate: row.end_date ? row.end_date.toISOString().split('T')[0] : null,
      statusProgression: [row.status],
      linkedContainers: row.container_number ? row.container_number : 'N/A',
      remarks: row.remarks || `Status: ${row.status}`
    }));

    console.log(`Fetched ${formattedHistory.length} history entries for order ${id}`);
    res.json(formattedHistory);
  } catch (err) {
    console.error("Error fetching order usage history:", err);
    res.status(500).json({ error: 'Failed to fetch order usage history' });
  }
}

export async function cancelOrder(req, res) {
  try {
    const { id } = req.params;
    const result = await pool.query('UPDATE orders SET status = \'Cancelled\', updated_at = CURRENT_TIMESTAMP WHERE id = $1 RETURNING *', [id]);

    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Order not found' });
    }

    // If associated container, insert 'Available' status
    const order = result.rows[0];
    if (order.associated_container) {
      const containerCheck = await pool.query(
        'SELECT cid FROM container_master WHERE container_number = $1 AND status = 1',
        [order.associated_container]
      );
      if (containerCheck.rowCount > 0) {
        const cid = containerCheck.rows[0].cid;
        await pool.query(
          'INSERT INTO container_status (cid, availability, status_notes, created_by) VALUES ($1, $2, $3, $4)',
          [cid, 'Available', `Unassigned due to order ${id} cancellation`, 'system']
        );
      }
    }

    console.log("Cancelled order:", id);
    res.json({ message: 'Order cancelled' });
  } catch (err) {
    console.error("pool error:", err.message);
    if (err.code === '23514') {
      return res.status(400).json({ error: 'Invalid value for constrained field' });
    }
    res.status(500).json({ error: err.message || 'Failed to cancel order' });
  }
}