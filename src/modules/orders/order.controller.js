import pool from "../../db/pool.js";



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

function normalizeDate(dateStr) {
  if (!dateStr || dateStr === '') return null;

  // Try strict YYYY-MM-DD first
  let parsed = new Date(dateStr);
  if (isNaN(parsed.getTime())) {
    // Fallback: Try parsing as MM/DD/YYYY (common US format)
    const parts = dateStr.split('/');
    if (parts.length === 3 && parts[0].length <= 2 && parts[1].length <= 2) {
      // Assume MM/DD/YYYY
      parsed = new Date(`${parts[2]}-${String(parseInt(parts[0])).padStart(2, '0')}-${String(parseInt(parts[1])).padStart(2, '0')}`);
    }
  }

  if (isNaN(parsed.getTime())) return null;

  return parsed.toISOString().split('T')[0]; // Always normalize to YYYY-MM-DD
}

// Also update isValidDate if it exists, to match:
function isValidDate(dateStr) {
  return normalizeDate(dateStr) !== null;
}


// Create a new order (handles multipart/form-data for files; aligned with updateOrder: consistent validation, normalization, optional container handling)
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

    // Parse receivers JSON (supports multiple; each with receiver_name, receiver_contact, etc.)
    let parsedReceivers = [];
    if (updates.receivers) {
      try {
        parsedReceivers = JSON.parse(updates.receivers);
      } catch (e) {
        console.warn('Failed to parse receivers:', e.message);
      }
    }
    if (parsedReceivers.length === 0) {
      throw new Error('receivers is required');
    }
    const firstReceiver = parsedReceivers[0] || {};
    // Set global consignment_marks from first receiver if not provided
    if (!updates.consignment_marks) {
      updates.consignment_marks = firstReceiver.consignment_marks || '';
    }
    if (parsedReceivers.length > 1) {
      console.log(`Multiple receivers detected (${parsedReceivers.length}); inserting all`);
    }

    // Normalize containers for each receiver: treat as single string → array for consistency
    parsedReceivers = parsedReceivers.map(rec => ({
      ...rec,
      containers: rec.containers ? (Array.isArray(rec.containers) ? rec.containers : [rec.containers]) : []
    }));

    // Parse order_items JSON (supports multiple; fallback to single from updates)
    let parsedItems = [];
    const hasOrderItemsJson = updates.order_items && typeof updates.order_items === 'string';
    if (hasOrderItemsJson) {
      try {
        parsedItems = JSON.parse(updates.order_items);
        // Optional: Map snake_case to camelCase if JSON uses snake (for consistency with code)
        parsedItems = parsedItems.map(item => ({
          category: item.category || item['category'] || '',
          subcategory: item.subcategory || item['subcategory'] || '',
          type: item.type || item['type'] || '',
          pickup_location: item.pickup_location || item['pickup_location'] || '',
          delivery_address: item.delivery_address || item['delivery_address'] || '',
          total_number: item.total_number || item['total_number'] || null,
          weight: item.weight || item['weight'] || null,
          item_ref: item.item_ref || item['item_ref'] || '',
          consignment_status: item.consignment_status || item['consignment_status'] || ''
        }));
      } catch (e) {
        console.warn('Failed to parse order_items:', e.message);
        parsedItems = [];
      }
    }
    const isReplacingItems = parsedItems.length > 0; // For create, always "replacing" as new
    if (hasOrderItemsJson && parsedItems.length === 0) {
      throw new Error('order_items JSON is invalid or empty');
    }
    if (!isReplacingItems) {
      // Single item fallback from updates
      parsedItems = [{
        category: updates.category || '',
        subcategory: updates.subcategory || '',
        type: updates.type || '',
        pickup_location: updates.pickup_location || '',
        delivery_address: updates.delivery_address || '',
        total_number: updates.total_number || null,
        weight: updates.weight || null,
        item_ref: updates.item_ref || '',
        consignment_status: ''
      }];
    }
    if (parsedItems.length === 0 || !parsedItems[0].category || !parsedItems[0].type) {
      throw new Error('At least one order_item is required (provide category, type, etc.)');
    }

    // Handle attachments for orders (JSONB; starts empty, like update's current)
    let newAttachments = [];
    let existingAttachmentsFromForm = [];
    if (updates.attachments_existing) {
      try {
        existingAttachmentsFromForm = JSON.parse(updates.attachments_existing);
      } catch (e) {
        console.warn('Failed to parse attachments_existing:', e.message);
      }
    }
    newAttachments = existingAttachmentsFromForm.length > 0 ? existingAttachmentsFromForm : newAttachments;
    if (files.attachments && files.attachments.length > 0) {
      const uploadedPaths = await uploadFiles(files.attachments, 'attachments'); // Assume uploadFiles returns array of paths
      newAttachments = [...newAttachments, ...uploadedPaths];
    }
    const attachmentsJson = JSON.stringify(newAttachments);

    // Handle gatepass for transport_details (JSONB; starts empty)
    let newGatepass = [];
    let existingGatepassFromForm = [];
    if (updates.gatepass_existing) {
      try {
        existingGatepassFromForm = JSON.parse(updates.gatepass_existing);
      } catch (e) {
        console.warn('Failed to parse gatepass_existing:', e.message);
      }
    }
    newGatepass = existingGatepassFromForm.length > 0 ? existingGatepassFromForm : newGatepass;
    if (files.gatepass && files.gatepass.length > 0) {
      const uploadedPaths = await uploadFiles(files.gatepass, 'gatepass');
      newGatepass = [...newGatepass, ...uploadedPaths];
    }
    const gatepassJson = JSON.stringify(newGatepass);

    // Allowed update fields (grouped by table; aligned with update)
    const ordersFields = ['booking_ref', 'status', 'eta', 'etd', 'place_of_loading', 'point_of_origin', 'final_destination', 'place_of_delivery', 'order_remarks', 'shipping_line', 'consignment_marks', 'consignment_remarks', 'rgl_booking_number', 'attachments'];
    const sendersFields = ['sender_name', 'sender_contact', 'sender_address', 'sender_email', 'sender_ref', 'sender_remarks'];
    const orderItemsFields = ['category', 'subcategory', 'type', 'pickup_location', 'delivery_address', 'total_number', 'weight', 'item_ref', 'consignment_status'];
    const transportFields = ['transport_type', 'third_party_transport', 'driver_name', 'driver_contact', 'driver_nic', 'driver_pickup_location', 'truck_number', 'drop_method', 'dropoff_name', 'drop_off_cnic', 'drop_off_mobile', 'plate_no', 'drop_date', 'collection_method', 'full_partial', 'qty_delivered', 'client_receiver_name', 'client_receiver_id', 'client_receiver_mobile', 'delivery_date', 'gatepass'];

    // Date keys
    const dateKeys = ['eta', 'etd', 'drop_date', 'delivery_date'];

    // Numeric fields
    const numericFields = ['weight', 'total_number', 'qty_delivered', 'total_weight'];

    // Validation (aligned with update: use provided values as "effective")
    const updatedFields = {
      // Orders
      bookingRef: updates.booking_ref,
      status: updates.status || 'Created',
      eta: updates.eta,
      etd: updates.etd,
      placeOfLoading: updates.place_of_loading,
      pointOfOrigin: updates.point_of_origin,
      finalDestination: updates.final_destination,
      placeOfDelivery: updates.place_of_delivery,
      orderRemarks: updates.order_remarks,
      shippingLine: updates.shipping_line,
      rglBookingNumber: updates.rgl_booking_number,
      consignmentRemarks: updates.consignment_remarks,
      consignmentMarks: updates.consignment_marks,
      // Senders
      senderName: updates.sender_name,
      senderContact: updates.sender_contact,
      senderAddress: updates.sender_address,
      senderEmail: updates.sender_email,
      senderRef: updates.sender_ref,
      senderRemarks: updates.sender_remarks,
      // Transport
      transportType: updates.transport_type || 'Road',
      thirdPartyTransport: updates.third_party_transport,
      driverName: updates.driver_name,
      driverContact: updates.driver_contact,
      driverNic: updates.driver_nic,
      driverPickupLocation: updates.driver_pickup_location,
      truckNumber: updates.truck_number,
      dropMethod: updates.drop_method,
      dropoffName: updates.dropoff_name,
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

    // Required fields (core; effective value from provided/defaults)
    const requiredFields = [
      'bookingRef', 'rglBookingNumber', 'senderName', 'placeOfLoading', 'finalDestination'
    ];

    requiredFields.forEach(camelField => {
      const value = updatedFields[camelField];
      if (!value || !value.trim()) {
        const actualField = camelField.toLowerCase().replace(/([A-Z])/g, '_$1').toLowerCase();
        updateErrors.push(`${actualField} is required`);
      }
    });

    // Validate receivers (always replacing as new)
    if (parsedReceivers.length === 0) {
      updateErrors.push('At least one receiver is required');
    } else {
      parsedReceivers.forEach((rec, index) => {
        if (!rec.receiver_name?.trim()) updateErrors.push(`receiver_name required for receiver ${index + 1}`);
        if (!rec.consignment_number?.trim()) updateErrors.push(`consignment_number required for receiver ${index + 1}`);
        if (!rec.total_weight || parseFloat(rec.total_weight) <= 0) updateErrors.push(`total_weight must be positive for receiver ${index + 1}`);
        if (rec.receiver_email && !emailRegex.test(rec.receiver_email)) {
          updateErrors.push(`Invalid receiver ${index + 1} email format`);
        }
      });
    }

    // Validate items
    parsedItems.forEach((item, index) => {
      if (!item.category?.trim()) updateErrors.push(`category required for order_item ${index + 1}`);
      if (!item.type?.trim()) updateErrors.push(`type required for order_item ${index + 1}`);
      if (item.weight && (isNaN(parseFloat(item.weight)) || parseFloat(item.weight) <= 0)) updateErrors.push(`weight must be positive for order_item ${index + 1}`);
    });

    // Conditional validations (using effective values)
    const showInbound = updatedFields.finalDestination && updatedFields.finalDestination.includes('Karachi');
    const showOutbound = updatedFields.placeOfLoading && updatedFields.placeOfLoading.includes('Dubai');

    if (showInbound && updatedFields.dropMethod === 'Drop-Off') {
      if (!updatedFields.dropoffName || !updatedFields.dropoffName.trim()) updateErrors.push('dropoff_name required for Drop-Off');
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

    // Format validations (only if provided in updates)
    for (const [camelField, providedValue] of Object.entries(updates)) {
      if (providedValue !== undefined && providedValue !== null && providedValue.trim() !== '') {
        let actualField = camelField;
        if (dateKeys.includes(camelField)) {
          actualField = camelField;
        } else {
          actualField = camelField.toLowerCase().replace(/([A-Z])/g, '_$1').toLowerCase();
        }
        if (dateKeys.includes(actualField) && !isValidDate(providedValue)) {
          updateErrors.push(`${actualField} invalid date format (use YYYY-MM-DD)`);
        } else if (numericFields.includes(actualField) && (isNaN(providedValue) || parseFloat(providedValue) <= 0)) {
          updateErrors.push(`${actualField} must be a positive number`);
        } else if (actualField === 'sender_email' && !emailRegex.test(providedValue)) {
          updateErrors.push(`${actualField} invalid email format`);
        } else if (['drop_off_mobile', 'client_receiver_mobile'].includes(actualField) && !mobileRegex.test(providedValue.replace(/\D/g, ''))) {
          updateErrors.push(`${actualField} invalid mobile number (10-15 digits expected)`);
        }
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

    // Normalize dates (only if provided)
    const normEta = updates.eta ? normalizeDate(updates.eta) : null;
    const normEtd = updates.etd ? normalizeDate(updates.etd) : null;
    const normDropDate = updates.drop_date ? normalizeDate(updates.drop_date) : null;
    const normDeliveryDate = updates.delivery_date ? normalizeDate(updates.delivery_date) : null;

    // 1. Insert into orders
    const ordersValues = [
      updatedFields.bookingRef,
      updatedFields.status,
      updatedFields.rglBookingNumber,
      updatedFields.consignmentRemarks || '',
      updatedFields.placeOfLoading,
      updatedFields.pointOfOrigin || '',
      updatedFields.finalDestination,
      updatedFields.placeOfDelivery || '',
      updatedFields.orderRemarks || '',
      normEta,
      normEtd,
      updatedFields.shippingLine || '',
      updatedFields.consignmentMarks || '',
      attachmentsJson,
      created_by
    ];

    const ordersQuery = `
      INSERT INTO orders (
        booking_ref, status, rgl_booking_number, consignment_remarks,
        place_of_loading, point_of_origin, final_destination, place_of_delivery, order_remarks,
        eta, etd, shipping_line, consignment_marks, attachments, created_by
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
      RETURNING id, booking_ref, status, created_at
    `;

    console.log('Orders query: 15 params, Values length:', ordersValues.length);

    const ordersResult = await client.query(ordersQuery, ordersValues);
    const orderId = ordersResult.rows[0].id;
    const newOrder = ordersResult.rows[0];

    // 2. Insert into senders (1:1)
    const sendersValues = [
      orderId,
      updatedFields.senderName,
      updatedFields.senderContact || '',
      updatedFields.senderAddress || '',
      updatedFields.senderEmail || '',
      updatedFields.senderRef || '',
      updatedFields.senderRemarks || ''
    ];

    const sendersQuery = `
      INSERT INTO senders (
        order_id, sender_name, sender_contact, sender_address, sender_email, sender_ref, sender_remarks
      ) VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING id, sender_name
    `;

    const sendersResult = await client.query(sendersQuery, sendersValues);
    const senderId = sendersResult.rows[0].id;

    // 3. Insert into receivers (multiple)
    const receiverIds = [];
    const trackingData = []; // For order_tracking
    for (const rec of parsedReceivers) {
      const receiversQuery = `
        INSERT INTO receivers (
          order_id, receiver_name, receiver_contact, receiver_address, receiver_email,
          consignment_vessel, consignment_number, consignment_marks, consignment_voyage,
          total_number, total_weight, assignment, item_ref, receiver_ref, containers
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        RETURNING id, receiver_name, consignment_number
      `;

      const recContainersJson = JSON.stringify(rec.containers); // Already normalized to array
      const receiversValues = [
        orderId,
        rec.receiver_name || '',
        rec.receiver_contact || '',
        rec.receiver_address || '',
        rec.receiver_email || '',
        rec.consignment_vessel || '',
        rec.consignment_number || '',
        rec.consignment_marks || '',
        rec.consignment_voyage || '',
        rec.total_number || null,
        rec.total_weight || null,
        rec.assignment || '',
        rec.item_ref || '',
        rec.receiver_ref || '',
        recContainersJson
      ];

      const recResult = await client.query(receiversQuery, receiversValues);
      const receiverId = recResult.rows[0].id;
      receiverIds.push(receiverId);

      // Find container_id for tracking (first container) - FIXED to container_master and cid
      let containerId = null;
      if (rec.containers && rec.containers.length > 0) {
        try {
          const contQuery = await client.query(
            'SELECT cid FROM container_master WHERE container_number = $1',
            [rec.containers[0]]
          );
          if (contQuery.rowCount > 0) {
            containerId = contQuery.rows[0].cid;
          } else {
            console.warn(`Container ${rec.containers[0]} not found in container_master; skipping link.`);
          }
        } catch (contErr) {
          if (contErr.code === '42P01' || contErr.code === '42703') {
            console.warn('container_master table or column issue; skipping container link. Create/update the table to enable linking.');
          } else {
            throw contErr; // Re-throw other errors
          }
        }
      }
      trackingData.push({
        receiverId,
        receiverRef: rec.receiver_ref || '',
        consignmentNumber: rec.consignment_number || '',
        containerId,
        status: rec.status || updatedFields.status
      });
    }

    // 4. Insert into order_items (multiple)
    for (const item of parsedItems) {
      const totalWeight = (parseFloat(item.weight) || 0) * (parseInt(item.total_number) || 0);
      const itemsQuery = `
        INSERT INTO order_items (
          order_id, sender_id, category, subcategory, type, pickup_location,
          delivery_address, total_number, weight, total_weight, item_ref, consignment_status
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
      `;

      const itemsValues = [
        orderId,
        senderId,
        item.category || '',
        item.subcategory || '',
        item.type || '',
        item.pickup_location || '',
        item.delivery_address || '',
        item.total_number || null,
        item.weight || null,
        totalWeight,
        item.item_ref || '',
        item.consignment_status || ''
      ];

      await client.query(itemsQuery, itemsValues);
    }

    // 5. Insert into transport_details (1:1)
    const transportValues = [
      orderId,
      updatedFields.transportType,
      updatedFields.thirdPartyTransport || '',
      updatedFields.driverName || '',
      updatedFields.driverContact || '',
      updatedFields.driverNic || '',
      updatedFields.driverPickupLocation || '',
      updatedFields.truckNumber || '',
      updatedFields.dropMethod || '',
      updatedFields.dropoffName || '',
      updatedFields.dropOffCnic || '',
      updatedFields.dropOffMobile || '',
      updatedFields.plateNo || '',
      normDropDate,
      updatedFields.collectionMethod || '',
      updatedFields.fullPartial || '',
      updatedFields.qtyDelivered ? parseInt(updatedFields.qtyDelivered) : null,
      updatedFields.clientReceiverName || '',
      updatedFields.clientReceiverId || '',
      updatedFields.clientReceiverMobile || '',
      normDeliveryDate,
      gatepassJson
    ];

    const transportQuery = `
      INSERT INTO transport_details (
        order_id, transport_type, third_party_transport, driver_name, driver_contact,
        driver_nic, driver_pickup_location, truck_number, drop_method, dropoff_name,
        drop_off_cnic, drop_off_mobile, plate_no, drop_date, collection_method,
        full_partial, qty_delivered, client_receiver_name, client_receiver_id,
        client_receiver_mobile, delivery_date, gatepass
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
                $16, $17, $18, $19, $20, $21, $22)
      RETURNING id
    `;

    console.log('Transport query: 22 params, Values length:', transportValues.length);

    await client.query(transportQuery, transportValues);

    // 6. Insert into order_tracking (multiple per receiver)
    for (const track of trackingData) {
      const trackingQuery = `
        INSERT INTO order_tracking (
          order_id, sender_id, sender_ref, receiver_id, receiver_ref,
          container_id, consignment_number, status, created_by
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      `;

      const senderRef = updatedFields.senderRef || '';
      const trackingValues = [
        orderId,
        senderId,
        senderRef,
        track.receiverId,
        track.receiverRef,
        track.containerId,
        track.consignmentNumber,
        track.status, // Use per-receiver status
        created_by
      ];

      await client.query(trackingQuery, trackingValues);
    }

    // Backward compat: Handle container assignment (optional; warn if not found, no ROLLBACK)
    const firstReceiverContainer = firstReceiver.containers && firstReceiver.containers.length > 0 ? firstReceiver.containers[0] : null;
    const associatedContainer = updates.associated_container || firstReceiverContainer || null;
    if (associatedContainer) {
      try {
        const containerCheck = await client.query(
          'SELECT cid FROM container_master WHERE container_number = $1',
          [associatedContainer]
        );
        if (containerCheck.rowCount > 0) {
          const cid = containerCheck.rows[0].cid;
          const availability = getContainerAvailability(updatedFields.status);
          await client.query(
            'INSERT INTO container_status (cid, availability, status_notes, created_by) VALUES ($1, $2, $3, $4)',
            [cid, availability, `Assigned to order ${orderId} (${updatedFields.status})`, created_by]
          );
        } else {
          console.warn(`Associated container ${associatedContainer} not found in container_master; skipping assignment.`);
        }
      } catch (contErr) {
        if (contErr.code === '42P01' || contErr.code === '42703') {
          console.warn('container_master or container_status table/column issue; skipping assignment.');
        } else {
          await client.query('ROLLBACK');
          throw contErr;
        }
      }
    }

    await client.query('COMMIT');

    console.log("Created new order:", { id: orderId });

    // Fetch summary for response (fallback to manual JOIN if view is missing)
    let orderSummary = [];
    try {
      // Try the view first
      const summaryQuery = 'SELECT * FROM order_summary WHERE order_id = $1';
      const summaryResult = await client.query(summaryQuery, [orderId]);
      orderSummary = summaryResult.rows;
      console.log('Fetched summary from view:', orderSummary.length > 0 ? 'success' : 'empty');
    } catch (summaryErr) {
      console.warn('order_summary view fetch failed (likely missing):', summaryErr.message);
      // Fallback: Manual JOIN for basic summary (adjust columns as needed for your schema)
      const fallbackQuery = `
        SELECT 
          o.id as order_id, o.booking_ref, o.status, o.created_at,
          s.sender_name, s.sender_email,
          json_agg(json_build_object(
            'id', r.id, 'receiver_name', r.receiver_name, 'consignment_number', r.consignment_number,
            'total_weight', r.total_weight
          )) FILTER (WHERE r.id IS NOT NULL) as receivers,
          json_agg(json_build_object(
            'category', oi.category, 'type', oi.type, 'weight', oi.weight
          )) FILTER (WHERE oi.id IS NOT NULL) as order_items,
          td.transport_type, td.driver_name
        FROM orders o
        LEFT JOIN senders s ON o.id = s.order_id
        LEFT JOIN receivers r ON o.id = r.order_id
        LEFT JOIN order_items oi ON o.id = oi.order_id
        LEFT JOIN transport_details td ON o.id = td.order_id
        WHERE o.id = $1
        GROUP BY o.id, o.booking_ref, o.status, o.created_at, s.sender_name, s.sender_email, td.transport_type, td.driver_name
      `;
      const fallbackResult = await client.query(fallbackQuery, [orderId]);
      orderSummary = fallbackResult.rows;
      console.log('Fetched fallback summary:', orderSummary.length > 0 ? 'success' : 'empty');
    }

    res.status(201).json({
      message: 'Order created successfully',
      order: newOrder,
      senders: sendersResult.rows,
      summary: orderSummary
    });
  } catch (error) {
    if (client) {
      await client.query('ROLLBACK');
    }
    console.error('Error creating order:', error);
    // Enhanced error handling (PostgreSQL codes)
    if (error.code === '23505') {
      return res.status(409).json({ error: 'Booking reference already exists' });
    }
    if (error.code === '23514') {
      return res.status(400).json({ error: 'Invalid value for constrained field' });
    }
    if (error.code === '22007' || error.code === '22008') {
      return res.status(400).json({ error: 'Invalid date format' });
    }
    if (error.code === '42P01' || error.code === '42703') { // Table/column does not exist
      return res.status(500).json({ error: 'Database schema mismatch. Run migrations.' });
    }
    if (error.message.includes('receivers is required') || error.message.includes('order_item is required')) {
      return res.status(400).json({ error: 'Invalid create fields', details: error.message });
    }
    res.status(500).json({ error: 'Failed to create order', details: error.message });
  } finally {
    if (client) {
      client.release();
    }
  }
}

// Now includes status sync for receivers via DB trigger; status added to receivers INSERT
export async function updateOrder(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');

    const { id } = req.params;
    const updates = req.body;
    const files = req.files || {}; // Assuming multer .fields() or .any()
    const updated_by = updates.updated_by || 'system';

    // Debug log
    console.log('Order update body (key fields):', { booking_ref: updates.booking_ref, status: updates.status, eta: updates.eta, etd: updates.etd, shipping_line: updates.shipping_line });
    console.log('Files received:', Object.keys(files));

    // Fetch current order and related records
    const currentOrderResult = await client.query('SELECT * FROM orders WHERE id = $1', [id]);
    if (currentOrderResult.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Order not found' });
    }
    const currentOrder = currentOrderResult.rows[0];

    const currentSenderResult = await client.query('SELECT * FROM senders WHERE order_id = $1', [id]);
    const currentSender = currentSenderResult.rows[0] || {};

    const currentTransportResult = await client.query('SELECT * FROM transport_details WHERE order_id = $1', [id]);
    const currentTransport = currentTransportResult.rows[0] || {};

    // Parse current JSONB fields
    let currentAttachments = currentOrder.attachments || [];
    if (typeof currentOrder.attachments === 'string') {
      try {
        currentAttachments = JSON.parse(currentOrder.attachments);
      } catch (e) {
        currentAttachments = [];
      }
    }
    let currentGatepass = currentTransport.gatepass || [];
    if (typeof currentTransport.gatepass === 'string') {
      try {
        currentGatepass = JSON.parse(currentTransport.gatepass);
      } catch (e) {
        currentGatepass = [];
      }
    }

    // Handle attachments for orders
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
      newAttachments = existingAttachmentsFromForm;
    }
    if (files.attachments && files.attachments.length > 0) {
      const uploadedPaths = await uploadFiles(files.attachments, 'attachments');
      newAttachments = [...newAttachments, ...uploadedPaths];
    }
    const attachmentsJson = JSON.stringify(newAttachments);

    // Handle gatepass for transport_details
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
      newGatepass = existingGatepassFromForm;
    }
    if (files.gatepass && files.gatepass.length > 0) {
      const uploadedPaths = await uploadFiles(files.gatepass, 'gatepass');
      newGatepass = [...newGatepass, ...uploadedPaths];
    }
    const gatepassJson = JSON.stringify(newGatepass);

    // Parse receivers JSON for update (to handle multiple)
    let parsedReceivers = [];
    if (updates.receivers) {
      try {
        parsedReceivers = JSON.parse(updates.receivers);
      } catch (e) {
        console.warn('Failed to parse receivers:', e.message);
      }
    }
    const isReplacingReceivers = parsedReceivers.length > 0;

    // Parse order_items JSON (supports multiple; fallback to single from updates)
    let parsedItems = [];
    const hasOrderItemsJson = updates.order_items && typeof updates.order_items === 'string';
    if (hasOrderItemsJson) {
      try {
        parsedItems = JSON.parse(updates.order_items);
        // Optional: Map snake_case to camelCase if JSON uses snake (for consistency with code)
        parsedItems = parsedItems.map(item => ({
          category: item.category || item['category'] || '',
          subcategory: item.subcategory || item['subcategory'] || '',
          type: item.type || item['type'] || '',
          pickup_location: item.pickup_location || item['pickup_location'] || '',
          delivery_address: item.delivery_address || item['delivery_address'] || '',
          total_number: item.total_number || item['total_number'] || null,
          weight: item.weight || item['weight'] || null,
          item_ref: item.item_ref || item['item_ref'] || '',
          consignment_status: item.consignment_status || item['consignment_status'] || ''
        }));
      } catch (e) {
        console.warn('Failed to parse order_items:', e.message);
        parsedItems = [];
      }
    }
    const isReplacingItems = parsedItems.length > 0;
    if (hasOrderItemsJson && parsedItems.length === 0) {
      throw new Error('order_items JSON is invalid or empty');
    }
    if (!isReplacingItems) {
      // Single item fallback from updates or current
      const currentItemResult = await client.query('SELECT * FROM order_items WHERE order_id = $1 ORDER BY id LIMIT 1', [id]);
      const currentItem = currentItemResult.rows[0] || {};
      parsedItems = [{
        category: updates.category || currentItem.category || '',
        subcategory: updates.subcategory || currentItem.subcategory || '',
        type: updates.type || currentItem.type || '',
        pickup_location: updates.pickup_location || currentItem.pickup_location || '',
        delivery_address: updates.delivery_address || currentItem.delivery_address || '',
        total_number: updates.total_number !== undefined ? updates.total_number : currentItem.total_number || null,
        weight: updates.weight !== undefined ? updates.weight : currentItem.weight || null,
        item_ref: updates.item_ref || currentItem.item_ref || '',
        consignment_status: updates.consignment_status || currentItem.consignment_status || ''
      }];
    }
    if (parsedItems.length === 0 || !parsedItems[0].category || !parsedItems[0].type) {
      throw new Error('At least one order_item is required (provide category, type, etc.)');
    }

    // Allowed update fields (grouped by table)
    const ordersFields = ['booking_ref', 'status', 'eta', 'etd', 'place_of_loading', 'point_of_origin', 'final_destination', 'place_of_delivery', 'order_remarks', 'shipping_line', 'consignment_marks', 'consignment_remarks', 'rgl_booking_number', 'attachments'];
    const sendersFields = ['sender_name', 'sender_contact', 'sender_address', 'sender_email', 'sender_ref', 'sender_remarks'];
    const orderItemsFields = ['category', 'subcategory', 'type', 'pickup_location', 'delivery_address', 'total_number', 'weight', 'item_ref', 'consignment_status'];
    const transportFields = ['transport_type', 'third_party_transport', 'driver_name', 'driver_contact', 'driver_nic', 'driver_pickup_location', 'truck_number', 'drop_method', 'dropoff_name', 'drop_off_cnic', 'drop_off_mobile', 'plate_no', 'drop_date', 'collection_method', 'full_partial', 'qty_delivered', 'client_receiver_name', 'client_receiver_id', 'client_receiver_mobile', 'delivery_date', 'gatepass'];

    // Date keys
    const dateKeys = ['eta', 'etd', 'drop_date', 'delivery_date'];

    // Numeric fields
    const numericFields = ['weight', 'total_number', 'qty_delivered', 'total_weight'];

    // Validation (similar to create, using effective values: update or current)
    const updatedFields = {
      // Orders
      bookingRef: updates.booking_ref !== undefined ? updates.booking_ref : currentOrder.booking_ref,
      status: updates.status !== undefined ? updates.status : currentOrder.status,
      eta: updates.eta !== undefined ? updates.eta : currentOrder.eta,
      etd: updates.etd !== undefined ? updates.etd : currentOrder.etd,
      placeOfLoading: updates.place_of_loading !== undefined ? updates.place_of_loading : currentOrder.place_of_loading,
      pointOfOrigin: updates.point_of_origin !== undefined ? updates.point_of_origin : currentOrder.point_of_origin,
      finalDestination: updates.final_destination !== undefined ? updates.final_destination : currentOrder.final_destination,
      placeOfDelivery: updates.place_of_delivery !== undefined ? updates.place_of_delivery : currentOrder.place_of_delivery,
      orderRemarks: updates.order_remarks !== undefined ? updates.order_remarks : currentOrder.order_remarks,
      shippingLine: updates.shipping_line !== undefined ? updates.shipping_line : currentOrder.shipping_line,
      rglBookingNumber: updates.rgl_booking_number !== undefined ? updates.rgl_booking_number : currentOrder.rgl_booking_number,
      consignmentRemarks: updates.consignment_remarks !== undefined ? updates.consignment_remarks : currentOrder.consignment_remarks,
      consignmentMarks: updates.consignment_marks !== undefined ? updates.consignment_marks : currentOrder.consignment_marks,
      // Senders
      senderName: updates.sender_name !== undefined ? updates.sender_name : currentSender.sender_name,
      senderContact: updates.sender_contact !== undefined ? updates.sender_contact : currentSender.sender_contact,
      senderAddress: updates.sender_address !== undefined ? updates.sender_address : currentSender.sender_address,
      senderEmail: updates.sender_email !== undefined ? updates.sender_email : currentSender.sender_email,
      senderRef: updates.sender_ref !== undefined ? updates.sender_ref : currentSender.sender_ref,
      senderRemarks: updates.sender_remarks !== undefined ? updates.sender_remarks : currentSender.sender_remarks,
      // Transport
      transportType: updates.transport_type !== undefined ? updates.transport_type : currentTransport.transport_type,
      thirdPartyTransport: updates.third_party_transport !== undefined ? updates.third_party_transport : currentTransport.third_party_transport,
      driverName: updates.driver_name !== undefined ? updates.driver_name : currentTransport.driver_name,
      driverContact: updates.driver_contact !== undefined ? updates.driver_contact : currentTransport.driver_contact,
      driverNic: updates.driver_nic !== undefined ? updates.driver_nic : currentTransport.driver_nic,
      driverPickupLocation: updates.driver_pickup_location !== undefined ? updates.driver_pickup_location : currentTransport.driver_pickup_location,
      truckNumber: updates.truck_number !== undefined ? updates.truck_number : currentTransport.truck_number,
      dropMethod: updates.drop_method !== undefined ? updates.drop_method : currentTransport.drop_method,
      dropoffName: updates.dropoff_name !== undefined ? updates.dropoff_name : currentTransport.dropoff_name,
      dropOffCnic: updates.drop_off_cnic !== undefined ? updates.drop_off_cnic : currentTransport.drop_off_cnic,
      dropOffMobile: updates.drop_off_mobile !== undefined ? updates.drop_off_mobile : currentTransport.drop_off_mobile,
      plateNo: updates.plate_no !== undefined ? updates.plate_no : currentTransport.plate_no,
      dropDate: updates.drop_date !== undefined ? updates.drop_date : currentTransport.drop_date,
      collectionMethod: updates.collection_method !== undefined ? updates.collection_method : currentTransport.collection_method,
      fullPartial: updates.full_partial !== undefined ? updates.full_partial : currentTransport.full_partial,
      qtyDelivered: updates.qty_delivered !== undefined ? updates.qty_delivered : currentTransport.qty_delivered,
      clientReceiverName: updates.client_receiver_name !== undefined ? updates.client_receiver_name : currentTransport.client_receiver_name,
      clientReceiverId: updates.client_receiver_id !== undefined ? updates.client_receiver_id : currentTransport.client_receiver_id,
      clientReceiverMobile: updates.client_receiver_mobile !== undefined ? updates.client_receiver_mobile : currentTransport.client_receiver_mobile,
      deliveryDate: updates.delivery_date !== undefined ? updates.delivery_date : currentTransport.delivery_date,
    };
    const updateErrors = [];
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    const mobileRegex = /^\d{10,15}$/;

    // Required fields (core; effective value from update or current)
    const requiredFields = [
      'bookingRef', 'rglBookingNumber', 'senderName', 'placeOfLoading', 'finalDestination'
    ];

    requiredFields.forEach(camelField => {
      const value = updatedFields[camelField];
      if (!value || !value.trim()) {
        const actualField = camelField.toLowerCase().replace(/([A-Z])/g, '_$1').toLowerCase();
        updateErrors.push(`${actualField} is required`);
      }
    });

    // Validate receivers (if replacing or not) - Added status validation
    if (isReplacingReceivers) {
      if (parsedReceivers.length === 0) {
        updateErrors.push('At least one receiver is required');
      } else {
        parsedReceivers.forEach((rec, index) => {
          if (!rec.receiver_name?.trim()) updateErrors.push(`receiver_name required for receiver ${index + 1}`);
          if (!rec.consignment_number?.trim()) updateErrors.push(`consignment_number required for receiver ${index + 1}`);
          if (!rec.total_weight || parseFloat(rec.total_weight) <= 0) updateErrors.push(`total_weight must be positive for receiver ${index + 1}`);
          if (rec.receiver_email && !emailRegex.test(rec.receiver_email)) {
            updateErrors.push(`Invalid receiver ${index + 1} email format`);
          }
          // Optional: Validate status (e.g., must be 'Created', 'Delivered', etc.)
          if (rec.status && !['Created', 'Delivered', 'In Transit'].includes(rec.status)) {  // Adjust valid statuses as needed
            updateErrors.push(`Invalid status '${rec.status}' for receiver ${index + 1}`);
          }
        });
      }
    } else {
      // Validate existing first receiver if no replace
      const existingRecResult = await client.query('SELECT * FROM receivers WHERE order_id = $1 ORDER BY id LIMIT 1', [id]);
      const existingRec = existingRecResult.rows[0] || {};
      if (!existingRec.receiver_name?.trim()) updateErrors.push('receiver_name required');
      if (!existingRec.consignment_number?.trim()) updateErrors.push('consignment_number required');
      if (!existingRec.total_weight || parseFloat(existingRec.total_weight) <= 0) updateErrors.push('total_weight must be positive');
      if (existingRec.receiver_email && !emailRegex.test(existingRec.receiver_email)) {
        updateErrors.push('Invalid receiver email format');
      }
    }

    // Validate items
    if (isReplacingItems) {
      parsedItems.forEach((item, index) => {
        if (!item.category?.trim()) updateErrors.push(`category required for order_item ${index + 1}`);
        if (!item.type?.trim()) updateErrors.push(`type required for order_item ${index + 1}`);
        if (item.weight && (isNaN(parseFloat(item.weight)) || parseFloat(item.weight) <= 0)) updateErrors.push(`weight must be positive for order_item ${index + 1}`);
      });
    } else {
      // Single effective
      const currentItemResult = await client.query('SELECT * FROM order_items WHERE order_id = $1 ORDER BY id LIMIT 1', [id]);
      const currentItem = currentItemResult.rows[0] || {};
      const effCategory = updates.category !== undefined ? updates.category : currentItem.category || '';
      const effType = updates.type !== undefined ? updates.type : currentItem.type || '';
      const effWeight = updates.weight !== undefined ? updates.weight : currentItem.weight || 0;
      if (!effCategory.trim()) updateErrors.push('category required for order_item');
      if (!effType.trim()) updateErrors.push('type required for order_item');
      if (parseFloat(effWeight) <= 0 || isNaN(parseFloat(effWeight))) updateErrors.push('weight must be positive for order_item');
    }

    // Conditional validations (using effective values)
    const effectiveFinalDestination = updates.final_destination !== undefined ? updates.final_destination : currentOrder.final_destination;
    const showInbound = effectiveFinalDestination && effectiveFinalDestination.includes('Karachi');
    const effectivePlaceOfLoading = updates.place_of_loading !== undefined ? updates.place_of_loading : currentOrder.place_of_loading;
    const showOutbound = effectivePlaceOfLoading && effectivePlaceOfLoading.includes('Dubai');

    const effectiveDropMethod = updates.drop_method !== undefined ? updates.drop_method : currentTransport.drop_method;
    if (showInbound && effectiveDropMethod === 'Drop-Off') {
      const effectiveDropoffName = updates.dropoff_name !== undefined ? updates.dropoff_name : currentTransport.dropoff_name;
      if (!effectiveDropoffName || !effectiveDropoffName.trim()) updateErrors.push('dropoff_name required for Drop-Off');
      const effectiveDropOffCnic = updates.drop_off_cnic !== undefined ? updates.drop_off_cnic : currentTransport.drop_off_cnic;
      if (!effectiveDropOffCnic || !effectiveDropOffCnic.trim()) updateErrors.push('drop_off_cnic required for Drop-Off');
      const effectiveDropOffMobile = updates.drop_off_mobile !== undefined ? updates.drop_off_mobile : currentTransport.drop_off_mobile;
      if (!effectiveDropOffMobile || !effectiveDropOffMobile.trim()) updateErrors.push('drop_off_mobile required for Drop-Off');
    }
    if (showInbound) {
      const effectiveDropDate = updates.drop_date !== undefined ? updates.drop_date : currentTransport.drop_date;
      if (!effectiveDropDate || !effectiveDropDate.trim()) updateErrors.push('drop_date required');
    }

    if (showOutbound) {
      const effectiveDeliveryDate = updates.delivery_date !== undefined ? updates.delivery_date : currentTransport.delivery_date;
      if (!effectiveDeliveryDate || !effectiveDeliveryDate.trim()) updateErrors.push('delivery_date required');
    }
    const effectiveFullPartial = updates.full_partial !== undefined ? updates.full_partial : currentTransport.full_partial;
    if (showOutbound && effectiveFullPartial === 'Partial') {
      const effectiveQtyDelivered = updates.qty_delivered !== undefined ? updates.qty_delivered : currentTransport.qty_delivered;
      if (!effectiveQtyDelivered || !effectiveQtyDelivered.trim()) updateErrors.push('qty_delivered required for Partial delivery');
    }
    const effectiveCollectionMethod = updates.collection_method !== undefined ? updates.collection_method : currentTransport.collection_method;
    if (showOutbound && effectiveCollectionMethod === 'Collected by Client') {
      const effectiveClientReceiverName = updates.client_receiver_name !== undefined ? updates.client_receiver_name : currentTransport.client_receiver_name;
      if (!effectiveClientReceiverName || !effectiveClientReceiverName.trim()) updateErrors.push('client_receiver_name required for Client Collection');
      const effectiveClientReceiverId = updates.client_receiver_id !== undefined ? updates.client_receiver_id : currentTransport.client_receiver_id;
      if (!effectiveClientReceiverId || !effectiveClientReceiverId.trim()) updateErrors.push('client_receiver_id required for Client Collection');
      const effectiveClientReceiverMobile = updates.client_receiver_mobile !== undefined ? updates.client_receiver_mobile : currentTransport.client_receiver_mobile;
      if (!effectiveClientReceiverMobile || !effectiveClientReceiverMobile.trim()) updateErrors.push('client_receiver_mobile required for Client Collection');
    }

    // Format validations (only if provided in updates)
    for (const [camelField, providedValue] of Object.entries(updates)) {
      if (providedValue !== undefined && providedValue !== null && providedValue.trim() !== '') {
        let actualField = camelField;
        if (dateKeys.includes(camelField)) {
          actualField = camelField;
        } else {
          actualField = camelField.toLowerCase().replace(/([A-Z])/g, '_$1').toLowerCase();
        }
        if (dateKeys.includes(actualField) && !isValidDate(providedValue)) {
          updateErrors.push(`${actualField} invalid date format (use YYYY-MM-DD)`);
        } else if (numericFields.includes(actualField) && (isNaN(providedValue) || parseFloat(providedValue) <= 0)) {
          updateErrors.push(`${actualField} must be a positive number`);
        } else if (actualField === 'sender_email' && !emailRegex.test(providedValue)) {
          updateErrors.push(`${actualField} invalid email format`);
        } else if (['drop_off_mobile', 'client_receiver_mobile'].includes(actualField) && !mobileRegex.test(providedValue.replace(/\D/g, ''))) {
          updateErrors.push(`${actualField} invalid mobile number (10-15 digits expected)`);
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
    }

    // Normalize dates (only if provided)
    const normEta = updates.eta !== undefined ? normalizeDate(updates.eta) : currentOrder.eta;
    const normEtd = updates.etd !== undefined ? normalizeDate(updates.etd) : currentOrder.etd;
    const normDropDate = updates.drop_date !== undefined ? normalizeDate(updates.drop_date) : currentTransport.drop_date;
    const normDeliveryDate = updates.delivery_date !== undefined ? normalizeDate(updates.delivery_date) : currentTransport.delivery_date;

    // Status change flag
    const statusChanged = updates.status && updates.status !== currentOrder.status;
    const finalStatus = updates.status || currentOrder.status;

    // Update orders
    const ordersSet = [];
    const ordersValues = [];
    let ordersParamIndex = 1;
    ordersFields.forEach(field => {
      if (updates[field] !== undefined) {
        let val = updates[field];
        if (field === 'attachments') val = attachmentsJson;
        if (field === 'eta') val = normEta;
        if (field === 'etd') val = normEtd;
        ordersSet.push(`${field} = $${ordersParamIndex}`);
        ordersValues.push(val);
        ordersParamIndex++;
      }
    });
    if (ordersSet.length > 0) {
      ordersSet.push('updated_at = CURRENT_TIMESTAMP');
      ordersValues.push(id);
      const ordersQuery = `UPDATE orders SET ${ordersSet.join(', ')} WHERE id = $${ordersParamIndex} RETURNING *`;
      await client.query(ordersQuery, ordersValues);
    }

    // Update senders (1:1)
    const sendersSet = [];
    const sendersValues = [];
    let sendersParamIndex = 1;
    sendersFields.forEach(field => {
      if (updates[field] !== undefined) {
        sendersSet.push(`${field} = $${sendersParamIndex}`);
        sendersValues.push(updates[field]);
        sendersParamIndex++;
      }
    });
    if (sendersSet.length > 0 && currentSender.id) {
      sendersValues.push(currentSender.id);
      const sendersQuery = `UPDATE senders SET ${sendersSet.join(', ')} WHERE id = $${sendersParamIndex}`;
      await client.query(sendersQuery, sendersValues);
    }

    // Handle receivers: delete existing and insert new from parsed JSON if provided
    let receiverIds = [];
    let trackingData = [];
    if (isReplacingReceivers) {
      await client.query('DELETE FROM receivers WHERE order_id = $1', [id]);
      await client.query('DELETE FROM order_tracking WHERE order_id = $1 AND receiver_id IS NOT NULL', [id]);
      for (const rec of parsedReceivers) {
        // Updated INSERT to include status (defaults to finalStatus if not provided)
        const receiverStatus = rec.status || finalStatus;
        const receiversQuery = `
          INSERT INTO receivers (
            order_id, receiver_name, receiver_contact, receiver_address, receiver_email,
            consignment_vessel, consignment_number, consignment_marks, consignment_voyage,
            total_number, total_weight, assignment, item_ref, receiver_ref, containers, status
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
          RETURNING id, receiver_name, consignment_number
        `;

        const recContainersJson = JSON.stringify(rec.containers || []);
        const receiversValues = [
          id,
          rec.receiver_name || '',
          rec.receiver_contact || '',
          rec.receiver_address || '',
          rec.receiver_email || '',
          rec.consignment_vessel || '',
          rec.consignment_number || '',
          rec.consignment_marks || '',
          rec.consignment_voyage || '',
          rec.total_number || null,
          rec.total_weight || null,
          rec.assignment || '',
          rec.item_ref || '',
          rec.receiver_ref || '',
          recContainersJson,
          receiverStatus  // New: Include status
        ];

        const recResult = await client.query(receiversQuery, receiversValues);
        const receiverId = recResult.rows[0].id;
        receiverIds.push(receiverId);

        // Find container_id for tracking (first container) - FIXED to container_master and cid
        let containerId = null;
        if (rec.containers && rec.containers.length > 0) {
          try {
            const contQuery = await client.query(
              'SELECT cid FROM container_master WHERE container_number = $1',
              [rec.containers[0]]
            );
            if (contQuery.rowCount > 0) {
              containerId = contQuery.rows[0].cid;
            } else {
              console.warn(`Container ${rec.containers[0]} not found in container_master; skipping link.`);
            }
          } catch (contErr) {
            if (contErr.code === '42P01' || contErr.code === '42703') {
              console.warn('container_master table or column issue; skipping container link. Create/update the table to enable linking.');
            } else {
              throw contErr; // Re-throw other errors
            }
          }
        }
        trackingData.push({
          receiverId,
          receiverRef: rec.receiver_ref || '',
          consignmentNumber: rec.consignment_number || '',
          containerId,
          status: receiverStatus  // Use the same status for tracking (trigger will sync back if needed)
        });
      }
    }

    // Insert tracking for new receivers with final status
    if (isReplacingReceivers && trackingData.length > 0) {
      for (const track of trackingData) {
        const trackingQuery = `
          INSERT INTO order_tracking (
            order_id, sender_id, sender_ref, receiver_id, receiver_ref,
            container_id, consignment_number, status, created_by
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `;

        const senderRef = updates.sender_ref !== undefined ? updates.sender_ref : currentSender.sender_ref || '';
        const trackingValues = [
          id,
          currentSender.id,
          senderRef,
          track.receiverId,
          track.receiverRef,
          track.containerId,
          track.consignmentNumber,
          track.status, // Use per-receiver status (trigger handles sync to receiver)
          updated_by
        ];

        await client.query(trackingQuery, trackingValues);
      }
    }

    // Handle order_items: replace if JSON provided, else update single
    if (isReplacingItems) {
      await client.query('DELETE FROM order_items WHERE order_id = $1', [id]);
      for (const item of parsedItems) {
        const totalWeight = (parseFloat(item.weight) || 0) * (parseInt(item.total_number) || 0);
        const itemsQuery = `
          INSERT INTO order_items (
            order_id, sender_id, category, subcategory, type, pickup_location,
            delivery_address, total_number, weight, total_weight, item_ref, consignment_status
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        `;

        const itemsValues = [
          id,
          currentSender.id,
          item.category || '',
          item.subcategory || '',
          item.type || '',
          item.pickup_location || '',
          item.delivery_address || '',
          item.total_number || null,
          item.weight || null,
          totalWeight,
          item.item_ref || '',
          item.consignment_status || ''
        ];

        await client.query(itemsQuery, itemsValues);
      }
    } else {
      // Update single first item
      const currentItemResult = await client.query('SELECT * FROM order_items WHERE order_id = $1 ORDER BY id LIMIT 1', [id]);
      const currentItem = currentItemResult.rows[0] || {};
      const itemsSet = [];
      const itemsValues = [];
      let itemsParamIndex = 1;
      orderItemsFields.forEach(field => {
        if (updates[field] !== undefined) {
          itemsSet.push(`${field} = $${itemsParamIndex}`);
          itemsValues.push(updates[field]);
          itemsParamIndex++;
        }
      });
      if (itemsSet.length > 0) {
        if (updates.total_number !== undefined || updates.weight !== undefined) {
          const newTotalNum = parseInt(updates.total_number !== undefined ? updates.total_number : currentItem.total_number) || 0;
          const newWeight = parseFloat(updates.weight !== undefined ? updates.weight : currentItem.weight) || 0;
          const calcTotalWeight = newTotalNum * newWeight;
          itemsSet.push('total_weight = $' + itemsParamIndex);
          itemsValues.push(calcTotalWeight);
          itemsParamIndex++;
        }
        if (currentItem.id) {
          itemsValues.push(currentItem.id);
          const itemsQuery = `UPDATE order_items SET ${itemsSet.join(', ')} WHERE id = $${itemsParamIndex}`;
          await client.query(itemsQuery, itemsValues);
        }
      }
    }

    // Update transport_details
    const transportSet = [];
    const transportValues = [];
    let transportParamIndex = 1;
    transportFields.forEach(field => {
      if (updates[field] !== undefined) {
        let val = updates[field];
        if (field === 'gatepass') val = gatepassJson;
        if (field === 'drop_date') val = normDropDate;
        if (field === 'delivery_date') val = normDeliveryDate;
        if (field === 'qty_delivered') val = parseInt(val) || null;
        transportSet.push(`${field} = $${transportParamIndex}`);
        transportValues.push(val);
        transportParamIndex++;
      }
    });
    if (transportSet.length > 0 && currentTransport.id) {
      transportValues.push(currentTransport.id);
      const transportQuery = `UPDATE transport_details SET ${transportSet.join(', ')} WHERE id = $${transportParamIndex}`;
      await client.query(transportQuery, transportValues);
    }

    // If status changed and no receiver replace, insert new tracking for existing receivers
    // (Trigger will sync status to receivers automatically)
    if (statusChanged && !isReplacingReceivers) {
      const existingRecResult = await client.query('SELECT id, receiver_ref, consignment_number, containers FROM receivers WHERE order_id = $1', [id]);
      const receiversToUpdate = existingRecResult.rows;
      for (const rec of receiversToUpdate) {
        let containerId = null;
        const containersJson = rec.containers;
        let contNums = [];
        if (typeof containersJson === 'string') {
          try {
            contNums = JSON.parse(containersJson);
          } catch (e) {
            console.warn('Failed to parse containers JSON:', e.message);
            // If parse fails, treat as single string if non-empty
            if (containersJson.trim() && containersJson.trim() !== '[]') {
              contNums = [containersJson.trim()];
            }
          }
        } else if (Array.isArray(containersJson)) {
          contNums = containersJson;
        } else if (containersJson && typeof containersJson === 'string') {
          if (containersJson.trim() && containersJson.trim() !== '[]') {
            contNums = [containersJson.trim()];
          }
        }
        if (contNums.length > 0) {
          try {
            const contQuery = await client.query('SELECT cid FROM container_master WHERE container_number = $1', [contNums[0]]);
            if (contQuery.rowCount > 0) {
              containerId = contQuery.rows[0].cid;
            }
          } catch (contErr) {
            if (contErr.code === '42P01' || contErr.code === '42703') {
              console.warn('container_master issue; skipping container link.');
            } else {
              throw contErr;
            }
          }
        }

        const trackingQuery = `
          INSERT INTO order_tracking (
            order_id, sender_id, sender_ref, receiver_id, receiver_ref,
            container_id, consignment_number, status, created_by
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `;

        const senderRef = updates.sender_ref !== undefined ? updates.sender_ref : currentSender.sender_ref || '';
        const trackingValues = [
          id,
          currentSender.id,
          senderRef,
          rec.id,
          rec.receiver_ref,
          containerId,
          rec.consignment_number,
          updates.status,  // Insert new status; trigger syncs to receiver
          updated_by
        ];

        await client.query(trackingQuery, trackingValues);
      }
    }

    await client.query('COMMIT');

    // Refetch updated records after commit to ensure fresh data
    const updatedOrderResult = await client.query('SELECT * FROM orders WHERE id = $1', [id]);
    const updatedSenderResult = await client.query('SELECT * FROM senders WHERE order_id = $1', [id]);

    // Fetch updated summary for response (fallback to manual JOIN if view is missing; enhanced to align with getOrders fields)
    // Updated to include receiver status in summary
    let orderSummary = [];
    try {
      // Try the view first
      const summaryQuery = 'SELECT * FROM order_summary WHERE order_id = $1';
      const summaryResult = await client.query(summaryQuery, [id]);
      orderSummary = summaryResult.rows;
      console.log('Fetched summary from view:', orderSummary.length > 0 ? 'success' : 'empty');
    } catch (summaryErr) {
      console.warn('order_summary view fetch failed (likely missing):', summaryErr.message);
      // Fallback: Manual JOIN for basic summary (enhanced with containers, tracking status, etc. to align with getOrders)
      // Added receiver status aggregation
      const fallbackQuery = `
        SELECT 
          o.id as order_id, o.booking_ref, o.status, o.created_at, o.eta, o.etd, o.shipping_line,
          s.sender_name, s.sender_contact, s.sender_address, s.sender_email, s.sender_ref, s.sender_remarks,
          t.transport_type, t.third_party_transport, t.driver_name, t.driver_contact, t.driver_nic,
          t.driver_pickup_location, t.truck_number, t.drop_method, t.dropoff_name, t.drop_off_cnic,
          t.drop_off_mobile, t.plate_no, t.drop_date, t.collection_method, t.full_partial,
          t.qty_delivered, t.client_receiver_name, t.client_receiver_id, t.client_receiver_mobile, t.delivery_date,
          t.gatepass,
          ot.status AS tracking_status, ot.created_time AS tracking_created_time, ot.container_id,
          cm.container_number,
          rs.receiver_summary,
          STRING_AGG(DISTINCT r.status, ', ' ORDER BY r.status) AS receiver_status_summary,  -- New: Aggregated statuses
          rc.receiver_containers_json
        FROM orders o
        LEFT JOIN senders s ON o.id = s.order_id
        LEFT JOIN transport_details t ON o.id = t.order_id
        LEFT JOIN LATERAL (
          SELECT ot2.status, ot2.created_time, ot2.container_id
          FROM order_tracking ot2 
          WHERE ot2.order_id = o.id ORDER BY ot2.created_time DESC LIMIT 1
        ) ot ON true
        LEFT JOIN container_master cm ON ot.container_id = cm.cid
        LEFT JOIN LATERAL (
          SELECT STRING_AGG(DISTINCT r.receiver_name, ', ' ORDER BY r.receiver_name) AS receiver_summary
          FROM receivers r
          WHERE r.order_id = o.id AND r.receiver_name IS NOT NULL
        ) rs ON true
        LEFT JOIN receivers r ON o.id = r.order_id  -- Join for status
        LEFT JOIN LATERAL (
          SELECT STRING_AGG(DISTINCT cont, ', ' ORDER BY cont) AS receiver_containers_json
          FROM (
            SELECT jsonb_array_elements_text(
              CASE 
                WHEN jsonb_typeof(containers) = 'array' THEN containers 
                ELSE jsonb_build_array(containers) 
              END
            ) AS cont
            FROM receivers 
            WHERE order_id = o.id 
              AND containers IS NOT NULL 
              AND (
                (jsonb_typeof(containers) = 'array' AND jsonb_array_length(containers) > 0)
                OR (jsonb_typeof(containers) = 'string' AND containers::text != '' AND containers::text != '[]')
              )
          ) sub
        ) rc ON true
        WHERE o.id = $1
        GROUP BY o.id, o.booking_ref, o.status, o.created_at, o.eta, o.etd, o.shipping_line,
                 s.sender_name, s.sender_contact, s.sender_address, s.sender_email, s.sender_ref, s.sender_remarks,
                 t.transport_type, t.third_party_transport, t.driver_name, t.driver_contact, t.driver_nic,
                 t.driver_pickup_location, t.truck_number, t.drop_method, t.dropoff_name, t.drop_off_cnic,
                 t.drop_off_mobile, t.plate_no, t.drop_date, t.collection_method, t.full_partial,
                 t.qty_delivered, t.client_receiver_name, t.client_receiver_id, t.client_receiver_mobile, t.delivery_date,
                 t.gatepass, ot.status, ot.created_time, ot.container_id, cm.container_number,
                 rs.receiver_summary, rc.receiver_containers_json
      `;
      const fallbackResult = await client.query(fallbackQuery, [id]);
      orderSummary = fallbackResult.rows;
      console.log('Fetched fallback summary:', orderSummary.length > 0 ? 'success' : 'empty');
    }

    console.log("Updated order:", id);
    res.json({
      message: 'Order updated successfully',
      order: updatedOrderResult.rows[0],
      senders: updatedSenderResult.rows,
      summary: orderSummary
    });
  } catch (error) {
    if (client) {
      await client.query('ROLLBACK');
    }
    console.error('Error updating order:', error);
    // Enhanced error handling (PostgreSQL codes)
    if (error.code === '23505') {
      return res.status(409).json({ error: 'Booking reference already exists' });
    }
    if (error.code === '23514') {
      return res.status(400).json({ error: 'Invalid value for constrained field' });
    }
    if (error.code === '22007' || error.code === '22008') {
      return res.status(400).json({ error: 'Invalid date format' });
    }
    if (error.code === '42P01' || error.code === '42703') { // Table/column does not exist
      return res.status(500).json({ error: 'Database schema mismatch. Run migrations.' });
    }
    if (error.message.includes('receivers is required') || error.message.includes('order_item is required')) {
      return res.status(400).json({ error: 'Invalid update fields', details: error.message });
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

// For receiver-facing tracking page (limit sensitive fields if needed, e.g., hide full sender address)
export async function getOrderByTrackingId(req, res) {
  try {
    const { trackingId } = req.params; // e.g., consignment_number
    if (!trackingId || !trackingId.trim()) {
      return res.status(400).json({ error: 'Tracking ID is required' });
    }

    // Enhanced SELECT with receiver aggregations (similar to getOrders)
    let selectFields = [
      'o.*',
      's.sender_name, s.sender_contact, s.sender_email', // Limited sender info for receiver view
      't.transport_type, t.driver_name, t.driver_contact, t.truck_number, t.drop_method, t.delivery_date',
      'ot.status AS tracking_status, ot.created_time AS tracking_created_time',
      'ot.container_id',
      'cm.container_number',
      // Receiver details (full for the matched receiver)
      'r.receiver_name, r.receiver_contact, r.receiver_address, r.receiver_email, r.consignment_number, r.total_weight, r.status AS receiver_status',
      'rs.other_receivers_summary', // Summary of other receivers if multiple
      'rc.receiver_containers_json'
    ].join(', ');

    // Base joins + receivers and containers (filter by consignment_number)
    let joins = `
      LEFT JOIN senders s ON o.id = s.order_id
      LEFT JOIN transport_details t ON o.id = t.order_id
      LEFT JOIN receivers r ON o.id = r.order_id AND r.consignment_number ILIKE $2  -- Filter by trackingId
      LEFT JOIN LATERAL (
        SELECT ot2.status, ot2.created_time, ot2.container_id
        FROM order_tracking ot2 
        WHERE ot2.order_id = o.id AND ot2.consignment_number ILIKE $2  -- Also filter tracking
        ORDER BY ot2.created_time DESC LIMIT 1
      ) ot ON true
      LEFT JOIN container_master cm ON ot.container_id = cm.cid
      LEFT JOIN LATERAL (
        SELECT STRING_AGG(DISTINCT r2.receiver_name, ', ' ORDER BY r2.receiver_name) AS other_receivers_summary
        FROM receivers r2
        WHERE r2.order_id = o.id AND r2.id != r.id  -- Exclude the matched receiver
      ) rs ON true
      LEFT JOIN LATERAL (
        SELECT STRING_AGG(DISTINCT cont, ', ' ORDER BY cont) AS receiver_containers_json
        FROM (
          SELECT jsonb_array_elements_text(r.containers::jsonb) AS cont
          WHERE r.order_id = o.id AND r.consignment_number ILIKE $2
          AND r.containers IS NOT NULL AND r.containers != '[]' AND jsonb_array_length(r.containers::jsonb) > 0
        ) sub
      ) rc ON true
    `;

    // Build WHERE clause (base on receiver match)
    let whereClause = 'WHERE r.id IS NOT NULL';  // Ensure a receiver matches the trackingId

    // Main query (no pagination, single order)
    const mainQuery = `
      SELECT ${selectFields}
      FROM orders o
      ${joins}
      ${whereClause}
      GROUP BY o.id, s.sender_name, s.sender_contact, s.sender_email,
               t.transport_type, t.driver_name, t.driver_contact, t.truck_number, t.drop_method, t.delivery_date,
               ot.status, ot.created_time, ot.container_id, cm.container_number,
               r.receiver_name, r.receiver_contact, r.receiver_address, r.receiver_email, r.consignment_number, r.total_weight, r.status,
               rs.other_receivers_summary, rc.receiver_containers_json
      ORDER BY o.created_at DESC
    `;
    const queryParams = [trackingId];  // $1 for trackingId in ILIKE

    console.log('Generated tracking query:', mainQuery);  // Debug
    const orderResult = await pool.query(mainQuery, queryParams);

    if (orderResult.rowCount === 0) {
      return res.status(404).json({ error: 'Order not found for this tracking ID' });
    }

    const order = orderResult.rows[0];

    // Derive overall_status (latest tracking or receiver status)
    const derivedStatus = order.tracking_status || order.receiver_status || order.status || 'Created';

    // Enrich with color (reuse getOrderStatusColor if defined elsewhere)
    const enrichedOrder = {
      ...order,
      overall_status: derivedStatus,
      color: getOrderStatusColor ? getOrderStatusColor(derivedStatus) : '#default'  // Assume function exists
    };

    console.log(`Tracked order by ID ${trackingId}:`, enrichedOrder.booking_ref);

    res.json({
      data: enrichedOrder,
      message: 'Order tracked successfully'
    });
  } catch (err) {
    console.error('Error tracking order:', err);
    if (err.code === '42P01' || err.code === '42703') {
      return res.status(500).json({ error: 'Database schema mismatch. Check table/column names.' });
    }
    res.status(500).json({ error: 'Failed to track order', details: err.message });
  }
}

// For receiver-facing tracking page (limit sensitive fields if needed, e.g., hide full sender address)
export async function getOrderByItemRef(req, res) {
  try {
    const { itemRef } = req.params; // e.g., item_ref
    if (!itemRef || !itemRef.trim()) {
      return res.status(400).json({ error: 'Item Reference is required' });
    }

    // Enhanced SELECT with receiver aggregations (similar to getOrders)
    let selectFields = [
      'o.*',
      's.sender_name, s.sender_contact, s.sender_email', // Limited sender info for receiver view
      't.transport_type, t.driver_name, t.driver_contact, t.truck_number, t.drop_method, t.delivery_date',
      'ot.status AS tracking_status, ot.created_time AS tracking_created_time',
      'ot.container_id',
      'cm.container_number',
      // Receiver details (full for the matched receiver)
      'r.receiver_name, r.receiver_contact, r.receiver_address, r.receiver_email, r.item_ref, r.total_weight, r.status AS receiver_status',
      'rs.other_receivers_summary', // Summary of other receivers if multiple
      'rc.receiver_containers_json'
    ].join(', ');

    // Base joins + receivers and containers (filter by item_ref)
    let joins = `
      LEFT JOIN senders s ON o.id = s.order_id
      LEFT JOIN transport_details t ON o.id = t.order_id
      LEFT JOIN receivers r ON o.id = r.order_id AND r.item_ref ILIKE $1  -- Filter by itemRef (fixed to $1)
      LEFT JOIN LATERAL (
        SELECT ot2.status, ot2.created_time, ot2.container_id
        FROM order_tracking ot2 
        WHERE ot2.order_id = o.id AND ot2.receiver_id = r.id  -- Link via receiver_id
        ORDER BY ot2.created_time DESC LIMIT 1
      ) ot ON true
      LEFT JOIN container_master cm ON ot.container_id = cm.cid
      LEFT JOIN LATERAL (
        SELECT STRING_AGG(DISTINCT r2.receiver_name, ', ' ORDER BY r2.receiver_name) AS other_receivers_summary
        FROM receivers r2
        WHERE r2.order_id = o.id AND r2.id != r.id  -- Exclude the matched receiver
      ) rs ON true
      LEFT JOIN LATERAL (
        SELECT STRING_AGG(DISTINCT cont, ', ' ORDER BY cont) AS receiver_containers_json
        FROM (
          SELECT jsonb_array_elements_text(r.containers::jsonb) AS cont
          WHERE r.order_id = o.id AND r.item_ref ILIKE $1  -- Fixed to $1
          AND r.containers IS NOT NULL AND r.containers != '[]' AND jsonb_array_length(r.containers::jsonb) > 0
        ) sub
      ) rc ON true
    `;

    // Build WHERE clause (base on receiver match)
    let whereClause = 'WHERE r.id IS NOT NULL';  // Ensure a receiver matches the itemRef

    // Main query (no pagination, single order)
    const mainQuery = `
      SELECT ${selectFields}
      FROM orders o
      ${joins}
      ${whereClause}
      GROUP BY o.id, s.sender_name, s.sender_contact, s.sender_email,
               t.transport_type, t.driver_name, t.driver_contact, t.truck_number, t.drop_method, t.delivery_date,
               ot.status, ot.created_time, ot.container_id, cm.container_number,
               r.receiver_name, r.receiver_contact, r.receiver_address, r.receiver_email, r.item_ref, r.total_weight, r.status,
               rs.other_receivers_summary, rc.receiver_containers_json
      ORDER BY o.created_at DESC
    `;
    const queryParams = [itemRef];  // $1 for itemRef in ILIKE

    console.log('Generated item_ref query:', mainQuery);  // Debug
    const orderResult = await pool.query(mainQuery, queryParams);

    if (orderResult.rowCount === 0) {
      return res.status(404).json({ error: 'Order not found for this Item Reference' });
    }

    const order = orderResult.rows[0];

    // Derive overall_status (latest tracking or receiver status)
    const derivedStatus = order.tracking_status || order.receiver_status || order.status || 'Created';

    // Enrich with color (reuse getOrderStatusColor if defined elsewhere)
    const enrichedOrder = {
      ...order,
      overall_status: derivedStatus,
      color: getOrderStatusColor ? getOrderStatusColor(derivedStatus) : '#default'  // Assume function exists
    };

    console.log(`Tracked order by Item Ref ${itemRef}:`, enrichedOrder.booking_ref);

    res.json({
      data: enrichedOrder,
      message: 'Order tracked successfully'
    });
  } catch (err) {
    console.error('Error tracking order by item_ref:', err);
    if (err.code === '42P01' || err.code === '42703') {
      return res.status(500).json({ error: 'Database schema mismatch. Check table/column names.' });
    }
    res.status(500).json({ error: 'Failed to track order', details: err.message });
  }
}

export async function getOrders(req, res) {
  try {
    const { page = 1, limit = 10, status, booking_ref, includeContainer = 'true' } = req.query;
    const offset = (page - 1) * limit;

    // Enhanced SELECT with receiver aggregations for table display (without status to avoid schema error)
    let selectFields = [
      'o.*',
      's.sender_name, s.sender_contact, s.sender_address, s.sender_email, s.sender_ref, s.sender_remarks',
      't.transport_type, t.third_party_transport, t.driver_name, t.driver_contact, t.driver_nic',
      't.driver_pickup_location, t.truck_number, t.drop_method, t.dropoff_name, t.drop_off_cnic',
      't.drop_off_mobile, t.plate_no, t.drop_date, t.collection_method, t.full_partial',
      't.qty_delivered, t.client_receiver_name, t.client_receiver_id, t.client_receiver_mobile, t.delivery_date',
      't.gatepass',
      'ot.status AS tracking_status, ot.created_time AS tracking_created_time',
      'ot.container_id',
      'cm.container_number',
      // Aggregated receivers summary as string (names only, until status column added)
      'rs.receiver_summary',
      // Aggregated containers as string
      'rc.receiver_containers_json'
    ].join(', ');

    // Base joins + new for receivers and containers
    let joins = `
      LEFT JOIN senders s ON o.id = s.order_id
      LEFT JOIN transport_details t ON o.id = t.order_id
      LEFT JOIN LATERAL (
        SELECT ot2.status, ot2.created_time, ot2.container_id
        FROM order_tracking ot2 
        WHERE ot2.order_id = o.id ORDER BY ot2.created_time DESC LIMIT 1
      ) ot ON true
      LEFT JOIN container_master cm ON ot.container_id = cm.cid
      LEFT JOIN LATERAL (
        SELECT STRING_AGG(DISTINCT r.receiver_name, ', ' ORDER BY r.receiver_name) AS receiver_summary
        FROM receivers r
        WHERE r.order_id = o.id AND r.receiver_name IS NOT NULL
      ) rs ON true
      LEFT JOIN LATERAL (
        SELECT STRING_AGG(DISTINCT cont, ', ' ORDER BY cont) AS receiver_containers_json
        FROM (
          SELECT jsonb_array_elements_text(
            CASE 
              WHEN jsonb_typeof(containers::jsonb) = 'array' THEN containers::jsonb 
              ELSE jsonb_build_array(containers::jsonb) 
            END
          ) AS cont
          FROM receivers 
          WHERE order_id = o.id 
            AND containers IS NOT NULL 
            AND containers != '[]'
            AND (
              (jsonb_typeof(containers::jsonb) = 'array' AND jsonb_array_length(containers::jsonb) > 0)
              OR (jsonb_typeof(containers::jsonb) = 'string' AND containers::text != '""' AND length(containers::text) > 2)
            )
        ) sub
      ) rc ON true
    `;

    // If includeContainer, extend for container status (assume sid exists; if not, set includeContainer='false' in frontend)
    let groupByExtra = '';
    if (includeContainer === 'true') {
      selectFields += `,
        cs.location AS container_location,
        cs.availability AS container_availability,
        COALESCE(NULL, 
          CASE 
            WHEN cs.availability = 'Cleared' THEN 'Cleared'
            ELSE cs.availability
          END
        ) AS container_derived_status
      `;
      groupByExtra = ', cs.location, cs.availability';
      const cmJoinPart = 'LEFT JOIN container_master cm ON ot.container_id = cm.cid';
      const cmJoinIndex = joins.indexOf(cmJoinPart);
      if (cmJoinIndex !== -1) {
        const newCmJoin = `${cmJoinPart}
         LEFT JOIN LATERAL (
           SELECT css.location, css.availability 
           FROM container_status css 
           WHERE css.cid = cm.cid ORDER BY css.sid DESC LIMIT 1
         ) cs ON true`;
        joins = joins.substring(0, cmJoinIndex) + newCmJoin + joins.substring(cmJoinIndex + cmJoinPart.length);
      }
    }

    // Build WHERE clause for filters (use o.status for now)
    let whereClause = 'WHERE 1=1';
    const queryParams = [];
    let paramIndex = 1;

    if (status) {
      whereClause += ` AND o.status = $${paramIndex}`;
      queryParams.push(status);
      paramIndex++;
    }
    if (booking_ref) {
      whereClause += ` AND o.booking_ref ILIKE $${paramIndex}`;
      queryParams.push(`%${booking_ref}%`);
      paramIndex++;
    }

    // Count query for pagination
    const countQuery = `
      SELECT COUNT(*) as total
      FROM orders o
      ${whereClause}
    `;
    const countResult = await pool.query(countQuery, queryParams);
    const total = parseInt(countResult.rows[0].total);

    // Main query with LIMIT/OFFSET
    const groupBy = `GROUP BY o.id, s.sender_name, s.sender_contact, s.sender_address, s.sender_email, s.sender_ref, s.sender_remarks,
               t.transport_type, t.third_party_transport, t.driver_name, t.driver_contact, t.driver_nic,
               t.driver_pickup_location, t.truck_number, t.drop_method, t.dropoff_name, t.drop_off_cnic,
               t.drop_off_mobile, t.plate_no, t.drop_date, t.collection_method, t.full_partial,
               t.qty_delivered, t.client_receiver_name, t.client_receiver_id, t.client_receiver_mobile, t.delivery_date,
               t.gatepass, ot.status, ot.created_time, ot.container_id, cm.container_number,
               rs.receiver_summary, rc.receiver_containers_json${groupByExtra}`;
    const mainQuery = `
      SELECT ${selectFields}
      FROM orders o
      ${joins}
      ${whereClause}
      ${groupBy}
      ORDER BY o.created_at DESC
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;
    queryParams.push(parseInt(limit), offset);
    console.log('Generated query:', mainQuery);  // Debug
    console.log('Query positions - Limit:', limit, 'Offset:', offset);
    console.log('Full Params length:', queryParams.length);

    const ordersResult = await pool.query(mainQuery, queryParams);

    // Derive overall_status in JS (use tracking_status as proxy for now)
    const orders = ordersResult.rows.map(order => {
      const derivedStatus = order.tracking_status || order.status || 'Created';
      return {
        ...order,
        overall_status: derivedStatus,
        color: getOrderStatusColor(derivedStatus)
      };
    });

    console.log(`Fetched orders: ${orders.length} Total: ${total} Filters:`, req.query);

    res.json({
      data: orders,  // Fixed: Match frontend expectation (response.data.data)
      total,
      page: parseInt(page),
      limit: parseInt(limit)
    });
  } catch (err) {
    console.error('Error fetching orders:', err);
    if (err.code === '42P01' || err.code === '42703') {
      return res.status(500).json({ error: 'Database schema mismatch. Check table/column names in query. Likely missing receivers.status or container_status.sid - run ALTER TABLE to add.' });
    }
    res.status(500).json({ error: 'Failed to fetch orders', details: err.message });
  }
}


export async function getOrderById(req, res) {
  try {
    const { id } = req.params;
    const { includeContainer = 'true' } = req.query;

    // Build SELECT fields dynamically
    let selectFields = [
      'o.*',
      's.sender_name, s.sender_contact, s.sender_address, s.sender_email, s.sender_ref, s.sender_remarks',
      't.transport_type, t.third_party_transport, t.driver_name, t.driver_contact, t.driver_nic',
      't.driver_pickup_location, t.truck_number, t.drop_method, t.dropoff_name, t.drop_off_cnic',
      't.drop_off_mobile, t.plate_no, t.drop_date, t.collection_method, t.full_partial',
      't.qty_delivered, t.client_receiver_name, t.client_receiver_id, t.client_receiver_mobile, t.delivery_date',
      't.gatepass',  // Added for parsing
      'ot.status AS tracking_status, ot.created_time AS tracking_created_time',  // Fixed to created_time
      'ot.container_id',  // Explicit for join
      'cm.container_number'  // From container_master
    ].join(', ');

    // Base joins
    let joins = `
      LEFT JOIN senders s ON o.id = s.order_id
      LEFT JOIN transport_details t ON o.id = t.order_id
      LEFT JOIN LATERAL (
        SELECT ot2.status, ot2.created_time, ot2.container_id
        FROM order_tracking ot2 
        WHERE ot2.order_id = o.id ORDER BY ot2.created_time DESC LIMIT 1
      ) ot ON true  -- Latest tracking with explicit fields
      LEFT JOIN container_master cm ON ot.container_id = cm.cid  -- Join to container_master on cid
    `;

    // If includeContainer, add SELECT fields and extend joins
    if (includeContainer === 'true') {
      selectFields += `,
        cs.location AS container_location,
        cs.availability AS container_availability,
        -- manual_derived_status if in schema
        COALESCE(NULL, 
          CASE 
            WHEN cs.availability = 'Cleared' THEN 'Cleared'
            ELSE cs.availability
          END
        ) AS container_derived_status
      `;
      // Extend joins for cs (safe string replacement)
      const cmJoinIndex = joins.indexOf('LEFT JOIN container_master cm ON ot.container_id = cm.cid');
      if (cmJoinIndex !== -1) {
        const newCmJoin = `LEFT JOIN container_master cm ON ot.container_id = cm.cid
         LEFT JOIN LATERAL (
           SELECT css.location, css.availability 
           FROM container_status css 
           WHERE css.cid = cm.cid ORDER BY css.sid DESC LIMIT 1  -- Assumed sid for sorting latest status
         ) cs ON true`;
        joins = joins.substring(0, cmJoinIndex) + newCmJoin;
      }
    }

    const query = `
      SELECT ${selectFields}
      FROM orders o
      ${joins}
      WHERE o.id = $1
      ORDER BY o.created_at DESC
    `;

    console.log('Generated query:', query);  // Debug log for verification

    const orderResult = await pool.query(query, [id]);
    if (orderResult.rowCount === 0) {
      return res.status(404).json({ error: 'Order not found' });
    }

    const orderRow = orderResult.rows[0];

    // Reverted to original: Fetch all receivers (assuming 'status' column now exists in receivers table)
    const receiversQuery = `
      SELECT * FROM receivers WHERE order_id = $1 ORDER BY id
    `;
    const receiversResult = await pool.query(receiversQuery, [id]);
    // In the receivers map (add console.log for testing)
    const receivers = receiversResult.rows.map(row => {
      let parsedContainers = row.containers || [];
      if (typeof row.containers === 'string') {
        if (row.containers.trim() === '') {
          parsedContainers = [];
        } else {
          try {
            parsedContainers = JSON.parse(row.containers);
          } catch (e) {
            parsedContainers = [row.containers];
          }
        }
      }
      const receiverStatus = row.status || 'Created';
      console.log(`Receiver ${row.id} raw status:`, row.status, '-> mapped to:', receiverStatus);  // Debug log
      return {
        ...row,
        status: receiverStatus,
        containers: Array.isArray(parsedContainers) ? parsedContainers : []
      };
    });
    // Fetch all order_items (multiple)
    const itemsQuery = `
      SELECT * FROM order_items WHERE order_id = $1 ORDER BY id
    `;
    const itemsResult = await pool.query(itemsQuery, [id]);
    const orderItems = itemsResult.rows;

    // Parse attachments (from orders) and gatepass (from transport_details)
    let parsedAttachments = orderRow.attachments || [];
    if (typeof orderRow.attachments === 'string') {
      if (orderRow.attachments.trim() === '') {
        parsedAttachments = [];
      } else {
        try {
          parsedAttachments = JSON.parse(orderRow.attachments);
        } catch (parseErr) {
          console.warn('Invalid JSON in attachments for order', id, '- treating as single path');
          parsedAttachments = [orderRow.attachments];  // Legacy scalar as single-element array
        }
      }
    }

    let parsedGatepass = orderRow.gatepass || [];
    if (typeof orderRow.gatepass === 'string') {
      if (orderRow.gatepass.trim() === '') {
        parsedGatepass = [];
      } else {
        try {
          parsedGatepass = JSON.parse(orderRow.gatepass);
        } catch (parseErr) {
          console.warn('Invalid JSON in gatepass for order', id, '- treating as single path');
          parsedGatepass = [orderRow.gatepass];  // Legacy scalar as single-element array
        }
      }
    }

    // Derive overall order status based on receivers' statuses
    // Assuming status hierarchy: Created < In Transit < Delivered < Completed
    // Overall status is the most advanced status among receivers
    let overallStatus = 'Created'; // Default if no receivers
    if (receivers.length > 0) {
      const receiverStatuses = receivers.map(r => r.status || 'Created');
      const statusOrder = { 'Created': 0, 'In Transit': 1, 'Delivered': 2, 'Completed': 3 };
      const maxStatusIndex = Math.max(...receiverStatuses.map(s => statusOrder[s] || 0));
      overallStatus = Object.keys(statusOrder).find(key => statusOrder[key] === maxStatusIndex);
    }

    const orderData = {
      ...orderRow,
      overall_status: overallStatus, // New field for derived status
      status: overallStatus, // Override existing status for backward compatibility
      attachments: Array.isArray(parsedAttachments) ? parsedAttachments : [],
      gatepass: Array.isArray(parsedGatepass) ? parsedGatepass : [],
      receivers,
      order_items: orderItems,
      color: getOrderStatusColor(overallStatus)
    };

    console.log(`Fetched order: ${orderData.booking_ref || id}`);
    res.json(orderData);
  } catch (err) {
    console.error("Error fetching order by ID:", err.message, "Params:", req.params);
    if (err.code === '23514') {
      return res.status(400).json({ error: 'Invalid value for constrained field' });
    }
    if (err.code === '42P01' || err.code === '42703') {
      return res.status(500).json({ error: 'Database schema mismatch. Check table/column names in query.' });
    }
    if (err.code === '42601') {  // Syntax error code
      return res.status(500).json({ error: 'SQL syntax error in query. Check logs for details.' });
    }
    res.status(500).json({ error: 'Failed to fetch order', details: err.message });
  }
}
// getOrderStatuses: Updated to fetch from order_tracking (merged statuses); group by order_id for history
export async function getOrderStatuses(req, res) {
  try {
    const { order_id } = req.params || req.query;  // Support both

    if (!order_id) {
      return res.status(400).json({ error: 'order_id is required' });
    }

    const query = `
      SELECT ot.id, ot.order_id, ot.status, ot.created_by, ot.created_time,
        r.receiver_name, r.receiver_ref, c.container_number
      FROM order_tracking ot
      LEFT JOIN receivers r ON ot.receiver_id = r.id
      LEFT JOIN containers c ON ot.container_id = c.id
      WHERE ot.order_id = $1
      ORDER BY ot.created_time ASC
    `;

    const result = await pool.query(query, [order_id]);
    const statuses = result.rows;

    console.log(`Fetched ${statuses.length} statuses for order: ${order_id}`);
    res.json({ statuses });
  } catch (err) {
    console.error("Error fetching order statuses:", err.message);
    res.status(500).json({ error: 'Failed to fetch order statuses', details: err.message });
  }
}

// getOrderUsageHistory: Updated to join order_tracking, transport_details for usage logs; adapt filters
export async function getOrderUsageHistory(req, res) {
  try {
    const { order_id } = req.params || req.query;

    if (!order_id) {
      return res.status(400).json({ error: 'order_id is required' });
    }

    const query = `
      SELECT 
        ot.created_time AS usage_time,
        ot.status AS usage_status,
        ot.created_by AS updated_by,
        t.driver_name, t.truck_number, t.drop_date AS transport_date,
        r.receiver_name, r.consignment_number
      FROM order_tracking ot
      LEFT JOIN transport_details t ON ot.order_id = t.order_id
      LEFT JOIN receivers r ON ot.receiver_id = r.id
      WHERE ot.order_id = $1
      ORDER BY ot.created_time ASC
    `;

    const result = await pool.query(query, [order_id]);
    const history = result.rows;

    console.log(`Fetched ${history.length} usage history for order: ${order_id}`);
    res.json({ history });
  } catch (err) {
    console.error("Error fetching order usage history:", err.message);
    res.status(500).json({ error: 'Failed to fetch order usage history', details: err.message });
  }
}

// cancelOrder: Updated to update orders.status and insert/update order_tracking for cancellation log
export async function cancelOrder(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');

    const { id } = req.params;
    const { reason } = req.body || {};  // Optional reason
    const updated_by = req.user?.id || 'system';  // Assume user from auth

    if (!id) {
      throw new Error('Order ID is required');
    }

    // Update global order status
    const updateQuery = `
      UPDATE orders 
      SET status = 'Cancelled', updated_at = CURRENT_TIMESTAMP, updated_by = $2
      WHERE id = $1 AND status != 'Cancelled'
      RETURNING id, booking_ref
    `;
    const updateResult = await client.query(updateQuery, [id, updated_by]);
    if (updateResult.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Order not found or already cancelled' });
    }

    // Log cancellation in order_tracking (for all receivers or latest)
    const trackingQuery = `
      INSERT INTO order_tracking (order_id, status, created_by, created_time)
      SELECT $1, 'Cancelled', $2, CURRENT_TIMESTAMP
      WHERE NOT EXISTS (SELECT 1 FROM order_tracking ot WHERE ot.order_id = $1 AND ot.status = 'Cancelled')
    `;
    await client.query(trackingQuery, [id, updated_by]);

    await client.query('COMMIT');
    console.log(`Cancelled order: ${updateResult.rows[0].booking_ref || id}, reason: ${reason || 'N/A'}`);
    res.json({ message: 'Order cancelled successfully', order_id: id, reason });
  } catch (err) {
    if (client) {
      await client.query('ROLLBACK');
    }
    console.error("Error cancelling order:", err.message);
    res.status(500).json({ error: 'Failed to cancel order', details: err.message });
  } finally {
    if (client) {
      client.release();
    }
  }
}
