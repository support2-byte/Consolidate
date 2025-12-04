import pool from "../../db/pool.js";
import sendOrderEmail from "../../middleware/nodeMailer.js";


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
  // if (!shippingLine) errors.push('shipping_line required');

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















// Helper function (assume defined elsewhere)
function normalizeDate(dateStr) {
  console.log('Normalizing date:', dateStr);
  if (!dateStr) return null;
  const date = new Date(dateStr);
  return isNaN(date.getTime()) ? null : date.toISOString().split('T')[0];
}

function isValidDate(dateStr) {
  return !isNaN(Date.parse(dateStr));
}

// Assume uploadFiles is defined
async function uploadFiles(files, type) {
  // Implementation for uploading files
  return files.map(f => `/uploads/${type}/${Date.now()}-${f.originalname}`);
}
export async function createOrder(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    const updates = req.body || {};
    const files = req.files || {}; // Assuming multer .fields() or .any()
    const created_by = 'system';
    // Map incoming snake_case to camelCase for consistency
    const camelUpdates = {
      bookingRef: updates.booking_ref,
      status: updates.status,
      rglBookingNumber: updates.rgl_booking_number,
      placeOfLoading: updates.place_of_loading,
      pointOfOrigin: updates.point_of_origin,
      finalDestination: updates.final_destination,
      placeOfDelivery: updates.place_of_delivery,
      orderRemarks: updates.order_remarks,
      eta: updates.eta,
      etd: updates.etd,
      // Sender fields
      senderName: updates.sender_name,
      senderContact: updates.sender_contact,
      senderAddress: updates.sender_address,
      senderEmail: updates.sender_email,
      senderRef: updates.sender_ref,
      senderRemarks: updates.sender_remarks,
      // Receiver fields (for when sender_type='receiver')
      receiverName: updates.receiver_name,
      receiverContact: updates.receiver_contact,
      receiverAddress: updates.receiver_address,
      receiverEmail: updates.receiver_email,
      receiverRef: updates.receiver_ref,
      receiverRemarks: updates.receiver_remarks,
      senderType: updates.sender_type,
      selectedSenderOwner: updates.selected_sender_owner || '',
      transportType: updates.transport_type,
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
      collectionScope: updates.collection_scope,
      qtyDelivered: updates.qty_delivered,
      clientReceiverName: updates.client_receiver_name,
      clientReceiverId: updates.client_receiver_id,
      clientReceiverMobile: updates.client_receiver_mobile,
      deliveryDate: updates.delivery_date,
      // JSON fields remain as-is
      receivers: updates.receivers,
      senders: updates.senders,
      order_items: updates.order_items,
      attachments_existing: updates.attachments_existing,
      gatepass_existing: updates.gatepass_existing,
    };
    // Debug log (using mapped camelCase)
    console.log('Order create body (key fields):', updates, {
      bookingRef: camelUpdates.bookingRef,
      status: camelUpdates.status,
      rglBookingNumber: camelUpdates.rglBookingNumber,
      pointOfOrigin: camelUpdates.pointOfOrigin,
      placeOfLoading: camelUpdates.placeOfLoading,
      placeOfDelivery: camelUpdates.placeOfDelivery,
      finalDestination: camelUpdates.finalDestination,
      orderRemarks: camelUpdates.orderRemarks,
      senderType: camelUpdates.senderType,
      selectedSenderOwner: camelUpdates.selectedSenderOwner,
      transportType: camelUpdates.transportType,
      dropMethod: camelUpdates.dropMethod,
      collectionMethod: camelUpdates.collectionMethod,
      collectionScope: camelUpdates.collectionScope,
      qtyDelivered: camelUpdates.qtyDelivered,
      receivers_sample: camelUpdates.receivers ? JSON.parse(camelUpdates.receivers).slice(0,1) : null,
      senders_sample: camelUpdates.senders ? JSON.parse(camelUpdates.senders).slice(0,1) : null,
      order_items_sample: camelUpdates.order_items ? JSON.parse(camelUpdates.order_items).slice(0,1) : null
    });
    console.log('Files received:', Object.keys(files));
    const senderType = camelUpdates.senderType || 'sender';
    // Parse shipping parties based on sender_type
    let parsedShippingParties = [];
    if (senderType === 'sender') {
      if (camelUpdates.receivers) {
        try {
          parsedShippingParties = JSON.parse(camelUpdates.receivers);
        } catch (e) {
          console.warn('Failed to parse receivers:', e.message);
        }
      }
    } else {
      if (camelUpdates.senders) {
        try {
          parsedShippingParties = JSON.parse(camelUpdates.senders);
        } catch (e) {
          console.warn('Failed to parse senders:', e.message);
        }
      }
    }
    if (parsedShippingParties.length === 0) {
      throw new Error(`Shipping parties (${senderType === 'sender' ? 'receivers' : 'senders'}) is required`);
    }
    // Parse flat order_items and group by party index extracted from item_ref
    let parsedShippingItems = [];
    try {
      const flatOrderItems = JSON.parse(camelUpdates.order_items || '[]');
      const itemsByParty = flatOrderItems.reduce((acc, item) => {
        if (item.item_ref) {
          const parts = item.item_ref.split('-');
          if (parts.length >= 6) {
            const partyIdx = parseInt(parts[3]) - 1;
            if (!isNaN(partyIdx) && partyIdx >= 0) {
              if (!acc[partyIdx]) acc[partyIdx] = [];
              acc[partyIdx].push(item);
            }
          } else {
            // MODIFICATION: Fallback - assign to first party (index 0) if item_ref is invalid/short
            console.warn(`Unparseable item_ref "${item.item_ref}" for item; assigning to party 0`);
            if (!acc[0]) acc[0] = [];
            acc[0].push(item);
          }
        } else {
          // MODIFICATION: If no item_ref at all, still assign to party 0
          console.warn(`No item_ref for item; assigning to party 0`);
          if (!acc[0]) acc[0] = [];
          acc[0].push(item);
        }
        return acc;
      }, {});
      // Initialize for all parties
      for (let i = 0; i < parsedShippingParties.length; i++) {
        parsedShippingItems[i] = itemsByParty[i] || [];
      }
    } catch (e) {
      console.warn('Failed to parse order_items:', e.message);
      parsedShippingItems = parsedShippingParties.map(() => []);
    }
    // MODIFICATION: Remove strict "hasValidShipping" check - allow parties with 0 items (treat as optional)
    // const hasValidShipping = parsedShippingParties.every((_, i) => parsedShippingItems[i].length > 0);
    // if (!hasValidShipping) {
    // throw new Error('Each shipping party must have at least one shipping detail');
    // }
    if (parsedShippingParties.length > 1) {
      console.log(`Multiple shipping parties detected (${parsedShippingParties.length}); inserting all with nested shipping details`);
    }
    // Map to receiver format if sender_type=receiver (swap roles)
    let parsedReceivers = parsedShippingParties;
    if (senderType === 'receiver') {
      parsedReceivers = parsedShippingParties.map(party => ({
        receiver_name: party.sender_name || party.senderName || '',
        receiver_contact: party.sender_contact || party.senderContact || '',
        receiver_address: party.sender_address || party.senderAddress || '',
        receiver_email: party.sender_email || party.senderEmail || '',
        status: party.status || 'Created',
        eta: party.eta,
        etd: party.etd,
        remarks: party.remarks || '',
        full_partial: party.full_partial || party.fullPartial || 'Full',
        qty_delivered: party.qty_delivered || party.qtyDelivered || 0,
      }));
    } else {
      parsedReceivers = parsedShippingParties.map(party => ({
        receiver_name: party.receiver_name || party.receiverName || '',
        receiver_contact: party.receiver_contact || party.receiverContact || '',
        receiver_address: party.receiver_address || party.receiverAddress || '',
        receiver_email: party.receiver_email || party.receiverEmail || '',
        status: party.status || 'Created',
        eta: party.eta,
        etd: party.etd,
        remarks: party.remarks || '',
        full_partial: party.full_partial || party.fullPartial || 'Full',
        qty_delivered: party.qty_delivered || party.qtyDelivered || 0,
      }));
    }
    // Aggregate totals from shipping items per receiver
    parsedReceivers = parsedReceivers.map((rec, i) => {
      const shippingDetails = parsedShippingItems[i] || [];
      const totalNum = shippingDetails.reduce((sum, item) => sum + (parseInt(item.total_number || 0) || 0), 0);
      const totalWt = shippingDetails.reduce((sum, item) => sum + (parseFloat(item.weight || 0) || 0), 0);
      return {
        ...rec,
        total_number: totalNum > 0 ? totalNum : null,
        total_weight: totalWt > 0 ? totalWt : null,
      };
    });
    // Handle attachments
    let newAttachments = [];
    let existingAttachmentsFromForm = [];
    if (camelUpdates.attachments_existing) {
      try {
        existingAttachmentsFromForm = JSON.parse(camelUpdates.attachments_existing);
      } catch (e) {
        console.warn('Failed to parse attachments_existing:', e.message);
      }
    }
    newAttachments = existingAttachmentsFromForm.length > 0 ? existingAttachmentsFromForm : newAttachments;
    if (files.attachments && files.attachments.length > 0) {
      const uploadedPaths = await uploadFiles(files.attachments, 'attachments');
      newAttachments = [...newAttachments, ...uploadedPaths];
    }
    const attachmentsJson = JSON.stringify(newAttachments);
    // Handle gatepass
    let newGatepass = [];
    let existingGatepassFromForm = [];
    if (camelUpdates.gatepass_existing) {
      try {
        existingGatepassFromForm = JSON.parse(camelUpdates.gatepass_existing);
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
    // Map owner fields based on sender_type
    let ownerName, ownerContact, ownerAddress, ownerEmail, ownerRef, ownerRemarks;
    if (senderType === 'sender') {
      ownerName = camelUpdates.senderName || '';
      ownerContact = camelUpdates.senderContact || '';
      ownerAddress = camelUpdates.senderAddress || '';
      ownerEmail = camelUpdates.senderEmail || '';
      ownerRef = camelUpdates.senderRef || '';
      ownerRemarks = camelUpdates.senderRemarks || '';
    } else {
      ownerName = camelUpdates.receiverName || '';
      ownerContact = camelUpdates.receiverContact || '';
      ownerAddress = camelUpdates.receiverAddress || '';
      ownerEmail = camelUpdates.receiverEmail || '';
      ownerRef = camelUpdates.receiverRef || '';
      ownerRemarks = camelUpdates.receiverRemarks || '';
    }
    // Updated fields matching UI (now using camelUpdates)
    const updatedFields = {
      bookingRef: camelUpdates.bookingRef,
      status: camelUpdates.status || 'Created',
      rglBookingNumber: camelUpdates.rglBookingNumber,
      placeOfLoading: camelUpdates.placeOfLoading,
      pointOfOrigin: camelUpdates.pointOfOrigin,
      finalDestination: camelUpdates.finalDestination,
      placeOfDelivery: camelUpdates.placeOfDelivery,
      orderRemarks: camelUpdates.orderRemarks,
      senderName: ownerName,
      senderContact: ownerContact,
      senderAddress: ownerAddress,
      senderEmail: ownerEmail,
      senderRef: ownerRef,
      senderRemarks: ownerRemarks,
      senderType: camelUpdates.senderType || 'sender',
      selectedSenderOwner: camelUpdates.selectedSenderOwner,
      transportType: camelUpdates.transportType,
      thirdPartyTransport: camelUpdates.thirdPartyTransport,
      driverName: camelUpdates.driverName,
      driverContact: camelUpdates.driverContact,
      driverNic: camelUpdates.driverNic,
      driverPickupLocation: camelUpdates.driverPickupLocation,
      truckNumber: camelUpdates.truckNumber,
      dropMethod: camelUpdates.dropMethod,
      dropoffName: camelUpdates.dropoffName,
      dropOffCnic: camelUpdates.dropOffCnic,
      dropOffMobile: camelUpdates.dropOffMobile,
      plateNo: camelUpdates.plateNo,
      dropDate: camelUpdates.dropDate,
      collectionMethod: camelUpdates.collectionMethod,
      collectionScope: camelUpdates.collectionScope,
      qtyDelivered: camelUpdates.qtyDelivered,
      clientReceiverName: camelUpdates.clientReceiverName,
      clientReceiverId: camelUpdates.clientReceiverId,
      clientReceiverMobile: camelUpdates.clientReceiverMobile,
      deliveryDate: camelUpdates.deliveryDate,
    };
    const updateErrors = [];
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    const mobileRegex = /^\d{10,15}$/;
    // MODIFICATION: Reduce required fields to bare minimum (e.g., drop pointOfOrigin, etc., if you want even looser)
    const requiredFields = [
      'rglBookingNumber', 'senderType' // Only these two as truly required for your sample
    ];
    requiredFields.forEach(camelField => {
      const value = updatedFields[camelField];
      if (!value || !value.trim()) {
        const actualField = camelField.toLowerCase().replace(/([A-Z])/g, '_$1').toLowerCase();
        updateErrors.push(`${actualField} is required`);
      }
    });
    if (updatedFields.senderType !== 'sender' && updatedFields.senderType !== 'receiver') {
      updateErrors.push('sender_type must be "sender" or "receiver"');
    }
    // MODIFICATION: Make owner fields optional (no errors if empty)
    // if (!ownerName?.trim()) updateErrors.push('owner_name is required');
    // if (!ownerContact?.trim()) updateErrors.push('owner_contact is required');
    // if (!ownerAddress?.trim()) updateErrors.push('owner_address is required');
    if (ownerEmail && !emailRegex.test(ownerEmail)) updateErrors.push('Invalid owner email format');
    if (parsedReceivers.length === 0) {
      updateErrors.push('At least one shipping party is required');
    } else {
      parsedReceivers.forEach((rec, index) => {
        const shippingDetails = parsedShippingItems[index] || [];
        // MODIFICATION: Make receiver basics optional (allow empty for minimal create)
        // if (!rec.receiver_name?.trim()) updateErrors.push(`receiver_name required for shipping party ${index + 1}`);
        // if (!rec.receiver_contact?.trim()) updateErrors.push(`receiver_contact required for shipping party ${index + 1}`);
        // if (!rec.receiver_address?.trim()) updateErrors.push(`receiver_address required for shipping party ${index + 1}`);
        if (rec.receiver_email && !emailRegex.test(rec.receiver_email)) updateErrors.push(`Invalid shipping party ${index + 1} email format`);
        // if (rec.receiver_contact && !mobileRegex.test(rec.receiver_contact.replace(/\D/g, ''))) updateErrors.push(`Invalid shipping party ${index + 1} contact format`);
        // MODIFICATION: Make eta/etd optional
        // if (!rec.eta) updateErrors.push(`eta required for shipping party ${index + 1}`);
        // if (!rec.etd) updateErrors.push(`etd required for shipping party ${index + 1}`);
        // MODIFICATION: Remove strict shipping details requirement - allow 0 items per party
        // if (shippingDetails.length === 0) {
        // updateErrors.push(`At least one shipping detail is required for shipping party ${index + 1}`);
        // } else {
        if (shippingDetails.length > 0) {
          shippingDetails.forEach((item, j) => {
            // MODIFICATION: Make item fields optional (warn but don't error)
            if (!item.pickup_location?.trim()) console.warn(`pickup_location missing for shipping detail ${j + 1} of party ${index + 1}`);
            if (!item.category?.trim()) console.warn(`category missing for shipping detail ${j + 1} of party ${index + 1}`);
            if (!item.subcategory?.trim()) console.warn(`subcategory missing for shipping detail ${j + 1} of party ${index + 1}`);
            if (!item.type?.trim()) console.warn(`type missing for shipping detail ${j + 1} of party ${index + 1}`);
            if (!item.delivery_address?.trim()) console.warn(`delivery_address missing for shipping detail ${j + 1} of party ${index + 1}`);
            const num = parseInt(item.total_number || 0);
            if (!item.total_number || num <= 0) console.warn(`total_number must be positive for shipping detail ${j + 1} of party ${index + 1}`);
            const wt = parseFloat(item.weight || 0);
            if (!item.weight || wt <= 0) console.warn(`weight must be positive for shipping detail ${j + 1} of party ${index + 1}`);
          });
        }
        // }
        // MODIFICATION: Relax partial validation - allow empty qty_delivered even for 'Partial'
        if (rec.full_partial === 'Partial') {
          const del = parseInt(rec.qty_delivered || 0);
          const recTotal = parseInt(rec.total_number || 0);
          if (del > recTotal && recTotal > 0) updateErrors.push(`qty_delivered cannot exceed total_number for party ${index + 1}`);
          // Removed: if (!rec.qty_delivered?.trim() || del <= 0) updateErrors.push(...);
        }
      });
    }
    if (updateErrors.length > 0) {
      console.warn('Create validation failed:', updateErrors);
      await client.query('ROLLBACK');
      return res.status(400).json({
        error: 'Invalid create fields',
        details: updateErrors.join('; ')
      });
    }
    const normDropDate = updatedFields.dropDate ? normalizeDate(updatedFields.dropDate) : null;
    const normDeliveryDate = updatedFields.deliveryDate ? normalizeDate(updatedFields.deliveryDate) : null;
    const ordersValues = [
      updatedFields.bookingRef,
      updatedFields.status,
      updatedFields.rglBookingNumber,
      updatedFields.placeOfLoading,
      updatedFields.pointOfOrigin,
      updatedFields.finalDestination,
      updatedFields.placeOfDelivery,
      updatedFields.orderRemarks || '',
      attachmentsJson,
      created_by
    ];
    const ordersQuery = `
      INSERT INTO orders (
        booking_ref, status, rgl_booking_number, place_of_loading, point_of_origin,
        final_destination, place_of_delivery, order_remarks, attachments, created_by
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      RETURNING id, booking_ref, status, created_at
    `;
    const ordersResult = await client.query(ordersQuery, ordersValues);
    const orderId = ordersResult.rows[0].id;
    const newOrder = ordersResult.rows[0];
    const sendersValues = [
      orderId,
      updatedFields.senderName,
      updatedFields.senderContact || '',
      updatedFields.senderAddress || '',
      updatedFields.senderEmail || '',
      updatedFields.senderRef || '',
      updatedFields.senderRemarks || '',
      updatedFields.senderType,
      updatedFields.selectedSenderOwner || ''
    ];
    const sendersQuery = `
      INSERT INTO senders (
        order_id, sender_name, sender_contact, sender_address, sender_email, sender_ref,
        sender_remarks, sender_type, selected_sender_owner
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      RETURNING id, sender_name
    `;
    const sendersResult = await client.query(sendersQuery, sendersValues);
    const senderId = sendersResult.rows[0].id;
    const receiverIds = [];
    const trackingData = [];
    for (let i = 0; i < parsedReceivers.length; i++) {
      const rec = parsedReceivers[i];
      const shippingDetails = parsedShippingItems[i];
      // ETA Integration: Calculate if not provided
      const recNormEta = rec.eta ? normalizeDate(rec.eta) : await calculateETA(client, rec.status || 'Created');
      const recNormEtd = rec.etd ? normalizeDate(rec.etd) : null;
      const receiversQuery = `
        INSERT INTO receivers (
          order_id, receiver_name, receiver_contact, receiver_address, receiver_email, eta, etd, shipping_line,
          consignment_vessel, consignment_number, consignment_marks, consignment_voyage,
          total_number, total_weight, remarks, containers, status, full_partial, qty_delivered
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
        RETURNING id, receiver_name
      `;
      const recContainersJson = JSON.stringify([]);
      const receiversValues = [
        orderId,
        rec.receiver_name || '',
        rec.receiver_contact || '',
        rec.receiver_address || '',
        rec.receiver_email || '',
        recNormEta,
        recNormEtd,
        '', // shipping_line
        '', // consignment_vessel
        '', // consignment_number
        '', // consignment_marks
        '', // consignment_voyage
        rec.total_number,
        rec.total_weight,
        rec.remarks || '',
        recContainersJson,
        rec.status || 'Created',
        rec.full_partial || 'Full',
        rec.qty_delivered ? parseInt(rec.qty_delivered) : null
      ];
      const recResult = await client.query(receiversQuery, receiversValues);
      const receiverId = recResult.rows[0].id;
      receiverIds.push(receiverId);
      // Insert multiple order_items per receiver
      for (let j = 0; j < shippingDetails.length; j++) {
        const item = shippingDetails[j];
        const orderItemsQuery = `
          INSERT INTO order_items (
            order_id, receiver_id, item_ref, pickup_location, delivery_address, category, subcategory, type,
            total_number, weight
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        `;
        const orderItemsValues = [
          orderId,
          receiverId,
          item.item_ref || '',
          item.pickup_location || '',
          item.delivery_address || '',
          item.category || '',
          item.subcategory || '',
          item.type || '',
          parseInt(item.total_number || 0),
          parseFloat(item.weight || 0)
        ];
        await client.query(orderItemsQuery, orderItemsValues);
      }
      let containerId = null;
      // Skip container lookup since not in UI
      trackingData.push({
        receiverId,
        status: rec.status || updatedFields.status,
        totalShippingDetails: shippingDetails.length
      });
    }
    // Insert transport details (updated based on UI fields)
    const transportQuery = `
      INSERT INTO transport_details (
        order_id, transport_type, drop_method, dropoff_name, drop_off_cnic, drop_off_mobile, plate_no, drop_date,
        collection_method, collection_scope, qty_delivered, client_receiver_name, client_receiver_id, client_receiver_mobile, delivery_date, gatepass,
        third_party_transport, driver_name, driver_contact, driver_nic, driver_pickup_location, truck_number
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
      RETURNING id
    `;
    const transportValues = [
      orderId,
      updatedFields.transportType,
      updatedFields.dropMethod || null,
      updatedFields.dropoffName || null,
      updatedFields.dropOffCnic || null,
      updatedFields.dropOffMobile || null,
      updatedFields.plateNo || null,
      normDropDate,
      updatedFields.collectionMethod || null,
      updatedFields.collectionScope || null,
      updatedFields.qtyDelivered ? parseInt(updatedFields.qtyDelivered) : null,
      updatedFields.clientReceiverName || null,
      updatedFields.clientReceiverId || null,
      updatedFields.clientReceiverMobile || null,
      normDeliveryDate,
      gatepassJson,
      updatedFields.thirdPartyTransport || null,
      updatedFields.driverName || null,
      updatedFields.driverContact || null,
      updatedFields.driverNic || null,
      updatedFields.driverPickupLocation || null,
      updatedFields.truckNumber || null
    ];
    await client.query(transportQuery, transportValues);
    await client.query('COMMIT');
    res.status(201).json({ success: true, order: newOrder, tracking: trackingData });
  } catch (error) {
    console.error('Error processing order:', error);
    if (client) await client.query('ROLLBACK');
    return res.status(500).json({ error: 'Internal server error', details: error.message });
  } finally {
    if (client) {
      client.release();
    }
  }
}

export async function updateOrder(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    const { id } = req.params;
    const updates = req.body || {};
    const files = req.files || {}; // Assuming multer .fields() or .any()
    const updated_by = updates.updated_by || 'system';
    // Fetch current order and related records for fallback
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
    // Map incoming snake_case to camelCase for consistency
    const camelUpdates = {
      bookingRef: updates.booking_ref,
      status: updates.status,
      rglBookingNumber: updates.rgl_booking_number,
      placeOfLoading: updates.place_of_loading,
      pointOfOrigin: updates.point_of_origin,
      finalDestination: updates.final_destination,
      placeOfDelivery: updates.place_of_delivery,
      orderRemarks: updates.order_remarks,
      eta: updates.eta,
      etd: updates.etd,
      // Sender fields
      senderName: updates.sender_name,
      senderContact: updates.sender_contact,
      senderAddress: updates.sender_address,
      senderEmail: updates.sender_email,
      senderRef: updates.sender_ref,
      senderRemarks: updates.sender_remarks,
      // Receiver fields (for when sender_type='receiver')
      receiverName: updates.receiver_name,
      receiverContact: updates.receiver_contact,
      receiverAddress: updates.receiver_address,
      receiverEmail: updates.receiver_email,
      receiverRef: updates.receiver_ref,
      receiverRemarks: updates.receiver_remarks,
      senderType: updates.sender_type,
      selectedSenderOwner: updates.selected_sender_owner || '',
      transportType: updates.transport_type,
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
      collectionScope: updates.collection_scope,
      qtyDelivered: updates.qty_delivered,
      clientReceiverName: updates.client_receiver_name,
      clientReceiverId: updates.client_receiver_id,
      clientReceiverMobile: updates.client_receiver_mobile,
      deliveryDate: updates.delivery_date,
      // JSON fields remain as-is
      receivers: updates.receivers,
      senders: updates.senders,
      order_items: updates.order_items,
      attachments_existing: updates.attachments_existing,
      gatepass_existing: updates.gatepass_existing,
    };
    // Debug log (using mapped camelCase)
    console.log('Order update body (key fields):', updates, {
      bookingRef: camelUpdates.bookingRef,
      status: camelUpdates.status,
      rglBookingNumber: camelUpdates.rglBookingNumber,
      pointOfOrigin: camelUpdates.pointOfOrigin,
      placeOfLoading: camelUpdates.placeOfLoading,
      placeOfDelivery: camelUpdates.placeOfDelivery,
      finalDestination: camelUpdates.finalDestination,
      orderRemarks: camelUpdates.orderRemarks,
      senderType: camelUpdates.senderType,
      selectedSenderOwner: camelUpdates.selectedSenderOwner,
      transportType: camelUpdates.transportType,
      dropMethod: camelUpdates.dropMethod,
      collectionMethod: camelUpdates.collectionMethod,
      collectionScope: camelUpdates.collectionScope,
      qtyDelivered: camelUpdates.qtyDelivered,
      receivers_sample: camelUpdates.receivers ? JSON.parse(camelUpdates.receivers).slice(0,1) : null,
      senders_sample: camelUpdates.senders ? JSON.parse(camelUpdates.senders).slice(0,1) : null,
      order_items_sample: camelUpdates.order_items ? JSON.parse(camelUpdates.order_items).slice(0,1) : null
    });
    console.log('Files received:', Object.keys(files));
    const senderType = camelUpdates.senderType || currentSender.sender_type || 'sender';
    // Parse shipping parties based on sender_type
    let parsedShippingParties = [];
    if (senderType === 'sender') {
      if (camelUpdates.receivers) {
        try {
          parsedShippingParties = JSON.parse(camelUpdates.receivers);
        } catch (e) {
          console.warn('Failed to parse receivers:', e.message);
        }
      }
    } else {
      if (camelUpdates.senders) {
        try {
          parsedShippingParties = JSON.parse(camelUpdates.senders);
        } catch (e) {
          console.warn('Failed to parse senders:', e.message);
        }
      }
    }
    const isReplacingShippingParties = parsedShippingParties.length > 0;
    if (!isReplacingShippingParties && parsedShippingParties.length === 0) {
      // Fallback to existing if not replacing
      const existingRecResult = await client.query('SELECT * FROM receivers WHERE order_id = $1 ORDER BY id', [id]);
      parsedShippingParties = existingRecResult.rows;
    }
    if (parsedShippingParties.length === 0) {
      throw new Error(`Shipping parties (${senderType === 'sender' ? 'receivers' : 'senders'}) is required`);
    }
    // Parse flat order_items and group by party index extracted from item_ref
    let parsedShippingItems = [];
    try {
      const flatOrderItems = JSON.parse(camelUpdates.order_items || '[]');
      const itemsByParty = flatOrderItems.reduce((acc, item) => {
        if (item.item_ref) {
          const parts = item.item_ref.split('-');
          if (parts.length >= 6) {
            const partyIdx = parseInt(parts[3]) - 1;
            if (!isNaN(partyIdx) && partyIdx >= 0) {
              if (!acc[partyIdx]) acc[partyIdx] = [];
              acc[partyIdx].push(item);
            }
          } else {
            // MODIFICATION: Fallback - assign to first party (index 0) if item_ref is invalid/short
            console.warn(`Unparseable item_ref "${item.item_ref}" for item; assigning to party 0`);
            if (!acc[0]) acc[0] = [];
            acc[0].push(item);
          }
        } else {
          // MODIFICATION: If no item_ref at all, still assign to party 0
          console.warn(`No item_ref for item; assigning to party 0`);
          if (!acc[0]) acc[0] = [];
          acc[0].push(item);
        }
        return acc;
      }, {});
      // Initialize for all parties
      for (let i = 0; i < parsedShippingParties.length; i++) {
        parsedShippingItems[i] = itemsByParty[i] || [];
      }
    } catch (e) {
      console.warn('Failed to parse order_items:', e.message);
      parsedShippingItems = parsedShippingParties.map(() => []);
    }
    // MODIFICATION: Remove strict "hasValidShipping" check - allow parties with 0 items (treat as optional)
    if (parsedShippingParties.length > 1) {
      console.log(`Multiple shipping parties detected (${parsedShippingParties.length}); updating all with nested shipping details`);
    }
    // Map to receiver format if sender_type=receiver (swap roles)
    let parsedReceivers = parsedShippingParties;
    if (senderType === 'receiver') {
      parsedReceivers = parsedShippingParties.map(party => ({
        receiver_name: party.sender_name || party.senderName || '',
        receiver_contact: party.sender_contact || party.senderContact || '',
        receiver_address: party.sender_address || party.senderAddress || '',
        receiver_email: party.sender_email || party.senderEmail || '',
        status: party.status || currentOrder.status || 'Created',
        eta: party.eta || currentOrder.eta,
        etd: party.etd || currentOrder.etd,
        remarks: party.remarks || '',
        full_partial: party.full_partial || party.fullPartial || 'Full',
        qty_delivered: party.qty_delivered || party.qtyDelivered || 0,
      }));
    } else {
      parsedReceivers = parsedShippingParties.map(party => ({
        receiver_name: party.receiver_name || party.receiverName || '',
        receiver_contact: party.receiver_contact || party.receiverContact || '',
        receiver_address: party.receiver_address || party.receiverAddress || '',
        receiver_email: party.receiver_email || party.receiverEmail || '',
        status: party.status || currentOrder.status || 'Created',
        eta: party.eta || currentOrder.eta,
        etd: party.etd || currentOrder.etd,
        remarks: party.remarks || '',
        full_partial: party.full_partial || party.fullPartial || 'Full',
        qty_delivered: party.qty_delivered || party.qtyDelivered || 0,
      }));
    }
    // Aggregate totals from shipping items per receiver
    parsedReceivers = parsedReceivers.map((rec, i) => {
      const shippingDetails = parsedShippingItems[i] || [];
      const totalNum = shippingDetails.reduce((sum, item) => sum + (parseInt(item.total_number || 0) || 0), 0);
      const totalWt = shippingDetails.reduce((sum, item) => sum + (parseFloat(item.weight || 0) || 0), 0);
      return {
        ...rec,
        total_number: totalNum > 0 ? totalNum : null,
        total_weight: totalWt > 0 ? totalWt : null,
      };
    });
    // Handle attachments (append to existing)
    let currentAttachments = currentOrder.attachments || [];
    if (typeof currentOrder.attachments === 'string') {
      try {
        currentAttachments = JSON.parse(currentOrder.attachments);
      } catch (e) {
        currentAttachments = [];
      }
    }
    let newAttachments = currentAttachments;
    let existingAttachmentsFromForm = [];
    if (camelUpdates.attachments_existing) {
      try {
        existingAttachmentsFromForm = JSON.parse(camelUpdates.attachments_existing);
      } catch (e) {
        console.warn('Failed to parse attachments_existing:', e.message);
      }
    }
    newAttachments = existingAttachmentsFromForm.length > 0 ? existingAttachmentsFromForm : newAttachments;
    if (files.attachments && files.attachments.length > 0) {
      const uploadedPaths = await uploadFiles(files.attachments, 'attachments');
      newAttachments = [...newAttachments, ...uploadedPaths];
    }
    const attachmentsJson = JSON.stringify(newAttachments);
    // Handle gatepass (append to existing)
    let currentGatepass = currentTransport.gatepass || [];
    if (typeof currentTransport.gatepass === 'string') {
      try {
        currentGatepass = JSON.parse(currentTransport.gatepass);
      } catch (e) {
        currentGatepass = [];
      }
    }
    let newGatepass = currentGatepass;
    let existingGatepassFromForm = [];
    if (camelUpdates.gatepass_existing) {
      try {
        existingGatepassFromForm = JSON.parse(camelUpdates.gatepass_existing);
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
    // Map owner fields based on sender_type
    let ownerName, ownerContact, ownerAddress, ownerEmail, ownerRef, ownerRemarks;
    if (senderType === 'sender') {
      ownerName = camelUpdates.senderName !== undefined ? camelUpdates.senderName : currentSender.sender_name || '';
      ownerContact = camelUpdates.senderContact !== undefined ? camelUpdates.senderContact : currentSender.sender_contact || '';
      ownerAddress = camelUpdates.senderAddress !== undefined ? camelUpdates.senderAddress : currentSender.sender_address || '';
      ownerEmail = camelUpdates.senderEmail !== undefined ? camelUpdates.senderEmail : currentSender.sender_email || '';
      ownerRef = camelUpdates.senderRef !== undefined ? camelUpdates.senderRef : currentSender.sender_ref || '';
      ownerRemarks = camelUpdates.senderRemarks !== undefined ? camelUpdates.senderRemarks : currentSender.sender_remarks || '';
    } else {
      ownerName = camelUpdates.receiverName !== undefined ? camelUpdates.receiverName : currentSender.sender_name || '';
      ownerContact = camelUpdates.receiverContact !== undefined ? camelUpdates.receiverContact : currentSender.sender_contact || '';
      ownerAddress = camelUpdates.receiverAddress !== undefined ? camelUpdates.receiverAddress : currentSender.sender_address || '';
      ownerEmail = camelUpdates.receiverEmail !== undefined ? camelUpdates.receiverEmail : currentSender.sender_email || '';
      ownerRef = camelUpdates.receiverRef !== undefined ? camelUpdates.receiverRef : currentSender.sender_ref || '';
      ownerRemarks = camelUpdates.receiverRemarks !== undefined ? camelUpdates.receiverRemarks : currentSender.sender_remarks || '';
    }
    // Updated fields matching UI (now using camelUpdates with current fallback)
    const updatedFields = {
      bookingRef: camelUpdates.bookingRef !== undefined ? camelUpdates.bookingRef : currentOrder.booking_ref,
      status: camelUpdates.status !== undefined ? camelUpdates.status : currentOrder.status,
      rglBookingNumber: camelUpdates.rglBookingNumber !== undefined ? camelUpdates.rglBookingNumber : currentOrder.rgl_booking_number,
      placeOfLoading: camelUpdates.placeOfLoading !== undefined ? camelUpdates.placeOfLoading : currentOrder.place_of_loading,
      pointOfOrigin: camelUpdates.pointOfOrigin !== undefined ? camelUpdates.pointOfOrigin : currentOrder.point_of_origin,
      finalDestination: camelUpdates.finalDestination !== undefined ? camelUpdates.finalDestination : currentOrder.final_destination,
      placeOfDelivery: camelUpdates.placeOfDelivery !== undefined ? camelUpdates.placeOfDelivery : currentOrder.place_of_delivery,
      orderRemarks: camelUpdates.orderRemarks !== undefined ? camelUpdates.orderRemarks : currentOrder.order_remarks,
      senderName: ownerName,
      senderContact: ownerContact,
      senderAddress: ownerAddress,
      senderEmail: ownerEmail,
      senderRef: ownerRef,
      senderRemarks: ownerRemarks,
      senderType: camelUpdates.senderType !== undefined ? camelUpdates.senderType : currentSender.sender_type || 'sender',
      selectedSenderOwner: camelUpdates.selectedSenderOwner !== undefined ? camelUpdates.selectedSenderOwner : currentSender.selected_sender_owner || '',
      transportType: camelUpdates.transportType !== undefined ? camelUpdates.transportType : currentTransport.transport_type,
      thirdPartyTransport: camelUpdates.thirdPartyTransport !== undefined ? camelUpdates.thirdPartyTransport : currentTransport.third_party_transport,
      driverName: camelUpdates.driverName !== undefined ? camelUpdates.driverName : currentTransport.driver_name,
      driverContact: camelUpdates.driverContact !== undefined ? camelUpdates.driverContact : currentTransport.driver_contact,
      driverNic: camelUpdates.driverNic !== undefined ? camelUpdates.driverNic : currentTransport.driver_nic,
      driverPickupLocation: camelUpdates.driverPickupLocation !== undefined ? camelUpdates.driverPickupLocation : currentTransport.driver_pickup_location,
      truckNumber: camelUpdates.truckNumber !== undefined ? camelUpdates.truckNumber : currentTransport.truck_number,
      dropMethod: camelUpdates.dropMethod !== undefined ? camelUpdates.dropMethod : currentTransport.drop_method,
      dropoffName: camelUpdates.dropoffName !== undefined ? camelUpdates.dropoffName : currentTransport.dropoff_name,
      dropOffCnic: camelUpdates.dropOffCnic !== undefined ? camelUpdates.dropOffCnic : currentTransport.drop_off_cnic,
      dropOffMobile: camelUpdates.dropOffMobile !== undefined ? camelUpdates.dropOffMobile : currentTransport.drop_off_mobile,
      plateNo: camelUpdates.plateNo !== undefined ? camelUpdates.plateNo : currentTransport.plate_no,
      dropDate: camelUpdates.dropDate !== undefined ? camelUpdates.dropDate : currentTransport.drop_date,
      collectionMethod: camelUpdates.collectionMethod !== undefined ? camelUpdates.collectionMethod : currentTransport.collection_method,
      collectionScope: camelUpdates.collectionScope !== undefined ? camelUpdates.collectionScope : currentTransport.collection_scope,
      qtyDelivered: camelUpdates.qtyDelivered !== undefined ? camelUpdates.qtyDelivered : currentTransport.qty_delivered,
      clientReceiverName: camelUpdates.clientReceiverName !== undefined ? camelUpdates.clientReceiverName : currentTransport.client_receiver_name,
      clientReceiverId: camelUpdates.clientReceiverId !== undefined ? camelUpdates.clientReceiverId : currentTransport.client_receiver_id,
      clientReceiverMobile: camelUpdates.clientReceiverMobile !== undefined ? camelUpdates.clientReceiverMobile : currentTransport.client_receiver_mobile,
      deliveryDate: camelUpdates.deliveryDate !== undefined ? camelUpdates.deliveryDate : currentTransport.delivery_date,
    };
    const updateErrors = [];
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    const mobileRegex = /^\d{10,15}$/;
    // MODIFICATION: Reduce required fields to bare minimum (e.g., drop pointOfOrigin, etc., if you want even looser)
    const requiredFields = [
      'rglBookingNumber', 'senderType' // Only these two as truly required for your sample
    ];
    requiredFields.forEach(camelField => {
      const value = updatedFields[camelField];
      if (!value || !value.trim()) {
        const actualField = camelField.toLowerCase().replace(/([A-Z])/g, '_$1').toLowerCase();
        updateErrors.push(`${actualField} is required`);
      }
    });
    if (updatedFields.senderType !== 'sender' && updatedFields.senderType !== 'receiver') {
      updateErrors.push('sender_type must be "sender" or "receiver"');
    }
    // MODIFICATION: Make owner fields optional (no errors if empty)
    // if (!ownerName?.trim()) updateErrors.push('owner_name is required');
    // if (!ownerContact?.trim()) updateErrors.push('owner_contact is required');
    // if (!ownerAddress?.trim()) updateErrors.push('owner_address is required');
    if (ownerEmail && !emailRegex.test(ownerEmail)) updateErrors.push('Invalid owner email format');
    if (parsedReceivers.length < 0) {
      updateErrors.push('At least one shipping party is required');s
    } else {
      parsedReceivers.forEach((rec, index) => {
        const shippingDetails = parsedShippingItems[index] || [];
        // MODIFICATION: Make receiver basics optional (allow empty for minimal update)
        // if (!rec.receiver_name?.trim()) updateErrors.push(`receiver_name required for shipping party ${index + 1}`);
        // if (!rec.receiver_contact?.trim()) updateErrors.push(`receiver_contact required for shipping party ${index + 1}`);
        // if (!rec.receiver_address?.trim()) updateErrors.push(`receiver_address required for shipping party ${index + 1}`);
        if (rec.receiver_email && !emailRegex.test(rec.receiver_email)) updateErrors.push(`Invalid shipping party ${index + 1} email format`);
        // if (rec.receiver_contact && !mobileRegex.test(rec.receiver_contact.replace(/\D/g, ''))) updateErrors.push(`Invalid shipping party ${index + 1} contact format`);
        // MODIFICATION: Make eta/etd optional
        // if (!rec.eta) updateErrors.push(`eta required for shipping party ${index + 1}`);
        // if (!rec.etd) updateErrors.push(`etd required for shipping party ${index + 1}`);
        // MODIFICATION: Remove strict shipping details requirement - allow 0 items per party
        // if (shippingDetails.length === 0) {
        // updateErrors.push(`At least one shipping detail is required for shipping party ${index + 1}`);
        // } else {
        if (shippingDetails.length > 0) {
          shippingDetails.forEach((item, j) => {
            // MODIFICATION: Make item fields optional (warn but don't error)
            if (!item.pickup_location?.trim()) console.warn(`pickup_location missing for shipping detail ${j + 1} of party ${index + 1}`);
            if (!item.category?.trim()) console.warn(`category missing for shipping detail ${j + 1} of party ${index + 1}`);
            if (!item.subcategory?.trim()) console.warn(`subcategory missing for shipping detail ${j + 1} of party ${index + 1}`);
            if (!item.type?.trim()) console.warn(`type missing for shipping detail ${j + 1} of party ${index + 1}`);
            if (!item.delivery_address?.trim()) console.warn(`delivery_address missing for shipping detail ${j + 1} of party ${index + 1}`);
            const num = parseInt(item.total_number || 0);
            if (!item.total_number || num <= 0) console.warn(`total_number must be positive for shipping detail ${j + 1} of party ${index + 1}`);
            const wt = parseFloat(item.weight || 0);
            if (!item.weight || wt <= 0) console.warn(`weight must be positive for shipping detail ${j + 1} of party ${index + 1}`);
          });
        }
        // }
        // MODIFICATION: Relax partial validation - allow empty qty_delivered even for 'Partial'
        if (rec.full_partial === 'Partial') {
          const del = parseInt(rec.qty_delivered || 0);
          const recTotal = parseInt(rec.total_number || 0);
          if (del > recTotal && recTotal > 0) updateErrors.push(`qty_delivered cannot exceed total_number for party ${index + 1}`);
          // Removed: if (!rec.qty_delivered?.trim() || del <= 0) updateErrors.push(...);
        }
      });
    }
    if (updateErrors.length > 0) {
      console.warn('Update validation failed:', updateErrors);
      await client.query('ROLLBACK');
      return res.status(400).json({
        error: 'Invalid update fields',
        details: updateErrors.join('; ')
      });
    }
    const normDropDate = updatedFields.dropDate ? normalizeDate(updatedFields.dropDate) : currentTransport.drop_date;
    const normDeliveryDate = updatedFields.deliveryDate ? normalizeDate(updatedFields.deliveryDate) : currentTransport.delivery_date;
    // Status change flag
    const statusChanged = camelUpdates.status && camelUpdates.status !== currentOrder.status;
    const finalStatus = camelUpdates.status || currentOrder.status;
    // Update orders table for provided fields
    const ordersSet = [];
    const ordersValues = [];
    let ordersParamIndex = 1;
    const ordersFields = [
      { key: 'booking_ref', val: updatedFields.bookingRef, param: 1 },
      { key: 'status', val: finalStatus, param: 1 },
      { key: 'rgl_booking_number', val: updatedFields.rglBookingNumber, param: 1 },
      { key: 'place_of_loading', val: updatedFields.placeOfLoading, param: 1 },
      { key: 'point_of_origin', val: updatedFields.pointOfOrigin, param: 1 },
      { key: 'final_destination', val: updatedFields.finalDestination, param: 1 },
      { key: 'place_of_delivery', val: updatedFields.placeOfDelivery, param: 1 },
      { key: 'order_remarks', val: updatedFields.orderRemarks || '', param: 1 },
      { key: 'attachments', val: attachmentsJson, param: 1 },
      { key: 'eta', val: camelUpdates.eta ? normalizeDate(camelUpdates.eta) : currentOrder.eta, param: 1 },
      { key: 'etd', val: camelUpdates.etd ? normalizeDate(camelUpdates.etd) : currentOrder.etd, param: 1 }
    ];
    ordersFields.forEach(field => {
      if (camelUpdates[field.key.replace(/_/g, '')] !== undefined || field.key === 'attachments') {
        ordersSet.push(`${field.key} = $${ordersParamIndex}`);
        ordersValues.push(field.val);
        ordersParamIndex++;
      }
    });
    if (ordersSet.length > 0) {
      ordersSet.push('updated_at = CURRENT_TIMESTAMP');
      ordersValues.push(id);
      const ordersQuery = `UPDATE orders SET ${ordersSet.join(', ')} WHERE id = $${ordersParamIndex} RETURNING *`;
      await client.query(ordersQuery, ordersValues);
    }
    // Update senders for provided owner fields
    const sendersSet = [];
    const sendersValues = [];
    let sendersParamIndex = 1;
    const senderFields = [
      { key: 'sender_name', val: ownerName },
      { key: 'sender_contact', val: ownerContact || '' },
      { key: 'sender_address', val: ownerAddress || '' },
      { key: 'sender_email', val: ownerEmail || '' },
      { key: 'sender_ref', val: ownerRef || '' },
      { key: 'sender_remarks', val: ownerRemarks || '' },
      { key: 'sender_type', val: updatedFields.senderType },
      { key: 'selected_sender_owner', val: updatedFields.selectedSenderOwner }
    ];
    senderFields.forEach(field => {
      const currentVal = currentSender[field.key];
      if (camelUpdates[field.key.replace(/_/g, '')] !== undefined || (field.key === 'sender_type' && camelUpdates.senderType !== undefined)) {
        sendersSet.push(`${field.key} = $${sendersParamIndex}`);
        sendersValues.push(field.val);
        sendersParamIndex++;
      }
    });
    if (sendersSet.length > 0 && currentSender.id) {
      sendersValues.push(currentSender.id);
      const sendersQuery = `UPDATE senders SET ${sendersSet.join(', ')} WHERE id = $${sendersParamIndex}`;
      await client.query(sendersQuery, sendersValues);
    }
    const receiverIds = [];
    const trackingData = [];
    // Handle receivers: replace if JSON provided, else skip or update individually if needed
    if (isReplacingShippingParties) {
      // Delete existing receivers and related items/tracking
      await client.query('DELETE FROM order_items WHERE order_id = $1', [id]);
      await client.query('DELETE FROM receivers WHERE order_id = $1', [id]);
      await client.query('DELETE FROM order_tracking WHERE order_id = $1', [id]);
    }
    for (let i = 0; i < parsedReceivers.length; i++) {
      const rec = parsedReceivers[i];
      const shippingDetails = parsedShippingItems[i];
      // ETA Integration: Calculate if not provided
      const recNormEta = rec.eta ? normalizeDate(rec.eta) : currentOrder.eta || await calculateETA(client, rec.status || finalStatus);
      const recNormEtd = rec.etd ? normalizeDate(rec.etd) : currentOrder.etd;
      let receiverId;
      if (isReplacingShippingParties) {
        const receiversQuery = `
          INSERT INTO receivers (
            order_id, receiver_name, receiver_contact, receiver_address, receiver_email, eta, etd, shipping_line,
            consignment_vessel, consignment_number, consignment_marks, consignment_voyage,
            total_number, total_weight, remarks, containers, status, full_partial, qty_delivered
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
          RETURNING id, receiver_name
        `;
        const recContainersJson = JSON.stringify([]);
        const receiversValues = [
          id,
          rec.receiver_name || '',
          rec.receiver_contact || '',
          rec.receiver_address || '',
          rec.receiver_email || '',
          recNormEta,
          recNormEtd,
          '', // shipping_line
          '', // consignment_vessel
          '', // consignment_number
          '', // consignment_marks
          '', // consignment_voyage
          rec.total_number,
          rec.total_weight,
          rec.remarks || '',
          recContainersJson,
          rec.status || finalStatus,
          rec.full_partial || 'Full',
          rec.qty_delivered ? parseInt(rec.qty_delivered) : null
        ];
        const recResult = await client.query(receiversQuery, receiversValues);
        receiverId = recResult.rows[0].id;
        receiverIds.push(receiverId);
      } else {
        // For non-replace, fetch existing receiver ID (assume first or by index, but for simplicity use first)
        const existingRecResult = await client.query('SELECT id FROM receivers WHERE order_id = $1 ORDER BY id LIMIT 1', [id]);
        receiverId = existingRecResult.rows[0]?.id;
        if (receiverId) {
          // Update existing receiver if fields changed
          const recSet = [];
          const recValues = [];
          let recParamIndex = 1;
          const recFields = [
            { key: 'receiver_name', val: rec.receiver_name || '' },
            { key: 'receiver_contact', val: rec.receiver_contact || '' },
            { key: 'receiver_address', val: rec.receiver_address || '' },
            { key: 'receiver_email', val: rec.receiver_email || '' },
            { key: 'eta', val: recNormEta },
            { key: 'etd', val: recNormEtd },
            { key: 'total_number', val: rec.total_number },
            { key: 'total_weight', val: rec.total_weight },
            { key: 'remarks', val: rec.remarks || '' },
            { key: 'status', val: rec.status || finalStatus },
            { key: 'full_partial', val: rec.full_partial || 'Full' },
            { key: 'qty_delivered', val: rec.qty_delivered ? parseInt(rec.qty_delivered) : null }
          ];
          recFields.forEach(field => {
            if (camelUpdates.receivers || camelUpdates.senders) { // Only if shipping JSON provided
              recSet.push(`${field.key} = $${recParamIndex}`);
              recValues.push(field.val);
              recParamIndex++;
            }
          });
          if (recSet.length > 0) {
            recValues.push(receiverId);
            const recQuery = `UPDATE receivers SET ${recSet.join(', ')} WHERE id = $${recParamIndex}`;
            await client.query(recQuery, recValues);
          }
        }
      }
      // Insert/Update multiple order_items per receiver
      if (isReplacingShippingParties) {
        for (let j = 0; j < shippingDetails.length; j++) {
          const item = shippingDetails[j];
          const orderItemsQuery = `
            INSERT INTO order_items (
              order_id, receiver_id, item_ref, pickup_location, delivery_address, category, subcategory, type,
              total_number, weight
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
          `;
          const orderItemsValues = [
            id,
            receiverId,
            item.item_ref || '',
            item.pickup_location || '',
            item.delivery_address || '',
            item.category || '',
            item.subcategory || '',
            item.type || '',
            parseInt(item.total_number || 0),
            parseFloat(item.weight || 0)
          ];
          await client.query(orderItemsQuery, orderItemsValues);
        }
      } else if (camelUpdates.order_items) {
        // For non-replace, delete and re-insert items if order_items provided
        await client.query('DELETE FROM order_items WHERE order_id = $1 AND receiver_id = $2', [id, receiverId]);
        for (let j = 0; j < shippingDetails.length; j++) {
          const item = shippingDetails[j];
          const orderItemsQuery = `
            INSERT INTO order_items (
              order_id, receiver_id, item_ref, pickup_location, delivery_address, category, subcategory, type,
              total_number, weight
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
          `;
          const orderItemsValues = [
            id,
            receiverId,
            item.item_ref || '',
            item.pickup_location || '',
            item.delivery_address || '',
            item.category || '',
            item.subcategory || '',
            item.type || '',
            parseInt(item.total_number || 0),
            parseFloat(item.weight || 0)
          ];
          await client.query(orderItemsQuery, orderItemsValues);
        }
      }
      let containerId = null;
      // Skip container lookup since not in UI
      trackingData.push({
        receiverId,
        status: rec.status || finalStatus,
        totalShippingDetails: shippingDetails.length
      });
    }
    // Insert tracking for new/updated parties if replacing or status changed
    if (isReplacingShippingParties || statusChanged) {
      await client.query('DELETE FROM order_tracking WHERE order_id = $1', [id]);
      for (const track of trackingData) {
        const trackingQuery = `
          INSERT INTO order_tracking (
            order_id, sender_id, sender_ref, receiver_id, receiver_ref,
            container_id, consignment_number, status, created_by
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `;
        const senderRef = updatedFields.senderRef;
        const consignmentNumber = ''; // Or generate if needed
        const trackingValues = [
          id,
          currentSender.id,
          senderRef,
          track.receiverId,
          '', // receiver_ref
          null, // container_id
          consignmentNumber,
          track.status,
          updated_by
        ];
        await client.query(trackingQuery, trackingValues);
      }
    }
    // Update transport details for provided fields
    const transportSet = [];
    const transportValues = [];
    let transportParamIndex = 1;
    const transportFields = [
      { key: 'transport_type', val: updatedFields.transportType },
      { key: 'third_party_transport', val: updatedFields.thirdPartyTransport },
      { key: 'driver_name', val: updatedFields.driverName },
      { key: 'driver_contact', val: updatedFields.driverContact },
      { key: 'driver_nic', val: updatedFields.driverNic },
      { key: 'driver_pickup_location', val: updatedFields.driverPickupLocation },
      { key: 'truck_number', val: updatedFields.truckNumber },
      { key: 'drop_method', val: updatedFields.dropMethod || null },
      { key: 'dropoff_name', val: updatedFields.dropoffName || null },
      { key: 'drop_off_cnic', val: updatedFields.dropOffCnic || null },
      { key: 'drop_off_mobile', val: updatedFields.dropOffMobile || null },
      { key: 'plate_no', val: updatedFields.plateNo || null },
      { key: 'drop_date', val: normDropDate },
      { key: 'collection_method', val: updatedFields.collectionMethod || null },
      { key: 'collection_scope', val: updatedFields.collectionScope || null },
      { key: 'qty_delivered', val: updatedFields.qtyDelivered ? parseInt(updatedFields.qtyDelivered) : null },
      { key: 'client_receiver_name', val: updatedFields.clientReceiverName || null },
      { key: 'client_receiver_id', val: updatedFields.clientReceiverId || null },
      { key: 'client_receiver_mobile', val: updatedFields.clientReceiverMobile || null },
      { key: 'delivery_date', val: normDeliveryDate },
      { key: 'gatepass', val: gatepassJson }
    ];
    transportFields.forEach(field => {
      const currentVal = currentTransport[field.key];
      if (camelUpdates[field.key.replace(/_/g, '')] !== undefined || field.key === 'gatepass') {
        transportSet.push(`${field.key} = $${transportParamIndex}`);
        transportValues.push(field.val);
        transportParamIndex++;
      }
    });
    if (transportSet.length > 0 && currentTransport.id) {
      transportValues.push(currentTransport.id);
      const transportQuery = `UPDATE transport_details SET ${transportSet.join(', ')} WHERE id = $${transportParamIndex}`;
      await client.query(transportQuery, transportValues);
    }
    await client.query('COMMIT');
    // Refetch updated data
    const updatedOrderResult = await client.query('SELECT * FROM orders WHERE id = $1', [id]);
    const updatedSenderResult = await client.query('SELECT * FROM senders WHERE order_id = $1', [id]);
    let orderSummary = [];
    try {
      const summaryQuery = 'SELECT * FROM order_summary WHERE order_id = $1';
      const summaryResult = await client.query(summaryQuery, [id]);
      orderSummary = summaryResult.rows;
    } catch (summaryErr) {
      console.warn('order_summary view fetch failed:', summaryErr.message);
      // Fallback query as in original
      const fallbackQuery = `
        SELECT
          o.id as order_id, o.booking_ref, o.status, o.created_at, o.eta, o.etd, o.shipping_line,
          s.sender_name, s.sender_contact, s.sender_address, s.sender_email, s.sender_ref,
          t.transport_type, t.third_party_transport, t.driver_name, t.driver_contact, t.driver_nic,
          t.driver_pickup_location, t.truck_number, t.drop_method, t.dropoff_name, t.drop_off_cnic,
          t.drop_off_mobile, t.plate_no, t.drop_date, t.collection_method,
          t.qty_delivered, t.client_receiver_name, t.client_receiver_id, t.client_receiver_mobile, t.delivery_date,
          t.gatepass,
          ot.status AS tracking_status, ot.created_time AS tracking_created_time, ot.container_id,
          cm.container_number,
          rs.receiver_summary,
          rss.receiver_status_summary,
          rc.receiver_containers_json,
          rt.total_items,
          rt.remaining_items,
          rd.receivers_details
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
        LEFT JOIN LATERAL (
          SELECT STRING_AGG(DISTINCT r2.status, ', ' ORDER BY r2.status) AS receiver_status_summary
          FROM receivers r2
          WHERE r2.order_id = o.id
        ) rss ON true
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
        LEFT JOIN LATERAL (
          SELECT
            COALESCE(SUM(total_number), 0) AS total_items,
            COALESCE(SUM(GREATEST(0, total_number - COALESCE(qty_delivered, 0))), 0) AS remaining_items
          FROM receivers
          WHERE order_id = o.id
        ) rt ON true
        LEFT JOIN LATERAL (
          SELECT json_agg(json_build_object(
            'id', r.id,
            'receiver_name', r.receiver_name,
            'total_number', r.total_number,
            'qty_delivered', r.qty_delivered,
            'remaining_items', GREATEST(0, r.total_number - COALESCE(r.qty_delivered, 0)),
            'full_partial', r.full_partial,
            'status', r.status
          ) ORDER BY r.id) AS receivers_details
          FROM receivers r
          WHERE r.order_id = o.id
        ) rd ON true
        WHERE o.id = $1
      `;
      const fallbackResult = await client.query(fallbackQuery, [id]);
      orderSummary = fallbackResult.rows;
    }
    console.log("Updated order:", id);
    res.json({
      message: 'Order updated successfully',
      order: updatedOrderResult.rows[0],
      senders: updatedSenderResult.rows,
      summary: orderSummary,
      tracking: trackingData
    });
  } catch (error) {
    console.error('Error updating order:', error);
    if (client) await client.query('ROLLBACK');
    if (error.code === '23505') {
      return res.status(409).json({ error: 'Booking reference already exists' });
    }
    if (error.code === '23514') {
      return res.status(400).json({ error: 'Invalid value for constrained field' });
    }
    if (error.code === '22007' || error.code === '22008') {
      return res.status(400).json({ error: 'Invalid date format' });
    }
    if (error.code === '42P01' || error.code === '42703') {
      return res.status(500).json({ error: 'Database schema mismatch. Run migrations.' });
    }
    return res.status(500).json({ error: 'Failed to update order', details: error.message });
  } finally {
    if (client) {
      client.release();
    }
  }
}
export async function getOrderById(req, res) {
  let client; // For potential tx if needed for calculateETA
  try {
    const { id } = req.params;
    const { includeContainer = 'true' } = req.query;

    client = await pool.connect(); // Connect for calculateETA if needed

    // Build SELECT fields dynamically (aligned with createOrder schema: orders + senders + transport_details)
    // Note: o.* includes total_assigned_qty if aggregated elsewhere; eta/etd not in orders, per receiver
    let selectFields = [
      'o.*',  // Core orders: booking_ref, status, rgl_booking_number, total_assigned_qty (if set), etc.
      's.sender_name, s.sender_contact, s.sender_address, s.sender_email, s.sender_ref, s.sender_remarks, s.sender_type, s.selected_sender_owner',  // From senders (aligned with create)
      't.transport_type, t.third_party_transport, t.driver_name, t.driver_contact, t.driver_nic',  // From transport_details
      't.driver_pickup_location, t.truck_number, t.drop_method, t.dropoff_name, t.drop_off_cnic',
      't.drop_off_mobile, t.plate_no, t.drop_date, t.collection_method, t.collection_scope, t.qty_delivered',  // From transport_details
      't.client_receiver_name, t.client_receiver_id, t.client_receiver_mobile, t.delivery_date',
      't.gatepass',  // From transport_details
      'ot.status AS tracking_status, ot.created_time AS tracking_created_time',  // Latest tracking
      'ot.container_id',  // Explicit for join
      'cm.container_number'  // From container_master (aligned with create's empty containers init)
    ].join(', ');

    // Build joins as array for easier extension
    let joinsArray = [
      'LEFT JOIN senders s ON o.id = s.order_id',
      'LEFT JOIN transport_details t ON o.id = t.order_id',
      'LEFT JOIN LATERAL (SELECT ot2.status, ot2.created_time, ot2.container_id FROM order_tracking ot2 WHERE ot2.order_id = o.id ORDER BY ot2.created_time DESC LIMIT 1) ot ON true',  // Latest tracking
      'LEFT JOIN container_master cm ON ot.container_id = cm.cid'  // Join to container_master on cid
    ];

    if (includeContainer === 'true') {
      selectFields += `,
        cs.location AS container_location,
        cs.availability AS container_availability,
        CASE 
          WHEN cs.availability = 'Cleared' THEN 'Cleared'
          ELSE cs.availability
        END AS container_derived_status
      `;
      joinsArray.push(
        'LEFT JOIN LATERAL (SELECT css.location, css.availability FROM container_status css WHERE css.cid = cm.cid ORDER BY css.sid DESC LIMIT 1) cs ON true'
      );
    }

    const joins = joinsArray.join('\n      ');

    const query = `
      SELECT ${selectFields}
      FROM orders o
      ${joins}
      WHERE o.id = $1
    `;

    const orderResult = await pool.query(query, [id]);
    if (orderResult.rowCount === 0) {
      return res.status(404).json({ error: 'Order not found' });
    }

    const orderRow = orderResult.rows[0];

    // Log total_assigned_qty for debugging (aligned with createOrder's aggregation potential)
    console.log(`[getOrderById ${id}] Order fetched with total_assigned_qty: ${orderRow.total_assigned_qty || 0}`);

    // Updated: Receivers query aligned with createOrder (eta/etd per receiver, containers init empty)
    const receiversQuery = `
      SELECT id, order_id, receiver_name, receiver_contact, receiver_address, receiver_email,
             total_number, total_weight, receiver_ref, remarks, containers,
             status,
             eta, etd, shipping_line, consignment_vessel, consignment_number, 
             consignment_marks, consignment_voyage, full_partial, qty_delivered
      FROM receivers WHERE order_id = $1 ORDER BY id
    `;
    const receiversResult = await pool.query(receiversQuery, [id]);
    
    console.log(`[getOrderById ${id}] Receivers fetched: ${receiversResult.rowCount} rows`);
    
    // Fetch all order_items for the order (multiple per receiver) - includes assigned_qty if column exists (aligned with create)
    const itemsQuery = `
      SELECT * FROM order_items WHERE order_id = $1 ORDER BY receiver_id, id
    `;
    const itemsResult = await pool.query(itemsQuery, [id]);
    const orderItems = itemsResult.rows;
    
    console.log(`[getOrderById ${id}] Order items fetched: ${orderItems.length} rows`);
    if (orderItems.length > 0) {
      console.log('[getOrderById] Sample item receiver_id:', orderItems[0]?.receiver_id);
    }

    // Group order_items by receiver_id (aligned with createOrder's per-receiver insertion)
    const itemsByReceiver = orderItems.reduce((acc, item) => {
      const rid = item.receiver_id;
      if (!acc[rid]) acc[rid] = [];
      acc[rid].push(item);
      return acc;
    }, {});

    let receivers = receiversResult.rows.map(async (row) => {  // Await for calculateETA if needed
      // Parse containers (enhanced: if single string and looks like container ID, treat as array with one item; aligned with create's empty init)
      let parsedContainers = [];
      if (row.containers) {
        try {
          parsedContainers = JSON.parse(row.containers);
          if (!Array.isArray(parsedContainers)) {
            parsedContainers = [parsedContainers];
          }
        } catch (e) {
          console.warn(`[getOrderById ${id}] Invalid JSON in containers for receiver ${row.id}: ${e.message}. Treating as single container: "${row.containers}"`);
          parsedContainers = [row.containers];  // Fallback to array with the string
        }
      }

      // Updated: shippingDetails as array from grouped order_items - now includes assigned_qty per detail (if exists; else total_number)
      const receiverItems = itemsByReceiver[row.id] || [];
      const shippingDetails = receiverItems.map(item => ({
        pickupLocation: item.pickup_location || '',
        deliveryAddress: item.delivery_address || '',
        category: item.category || '',
        subcategory: item.subcategory || '',
        type: item.type || '',
        totalNumber: item.total_number || '',
        weight: item.weight || '',
        itemRef: item.item_ref || '',
        assignedQty: item.assigned_qty || item.total_number || 0,  // Fallback to total_number if no assigned_qty
      }));

      // New: ETA logic - only show/calculate if container assigned (parsedContainers.length > 0)
      let formattedEta = null;
      let formattedEtd = null;
      if (parsedContainers.length > 0) {
        // Use stored eta if present; else calculate based on status (aligned with createOrder's calculateETA)
        if (row.eta) {
          formattedEta = new Date(row.eta).toISOString().split('T')[0];
        } else {
          formattedEta = await calculateETA(client, row.status || 'Created');
        }
        // ETD: Use stored if present; else null (no calc for ETD)
        if (row.etd) {
          formattedEtd = new Date(row.etd).toISOString().split('T')[0];
        }
      } else {
        console.log(`[getOrderById ${id}] No containers for receiver ${row.id}; eta/etd hidden (Container Required)`);
        formattedEta = null;  // Or 'Container Required' if you want a string
        formattedEtd = null;
      }

      // New log for debugging (extended for eta logic)
      console.log(`[getOrderById ${id}] Receiver ${row.id}: containers=${parsedContainers.length}, eta=${formattedEta || 'HIDDEN (No Container)'}, etd=${formattedEtd || 'EMPTY'}, shippingDetails count=${shippingDetails.length}`);

      // Format dates safely (now conditional for eta/etd)
      const getFormattedDate = (dateStr) => {
        if (!dateStr) return '';
        const date = new Date(dateStr);
        return isNaN(date.getTime()) ? '' : date.toISOString().split('T')[0];
      };

      const formattedRow = {
        ...row,
        eta: formattedEta,  // Conditional: only if container assigned
        etd: formattedEtd,  // Conditional: only if container assigned
        shipping_line: row.shipping_line || '',  // From row (aligned with create)
        consignment_vessel: row.consignment_vessel || '',  // From row
        consignment_number: row.consignment_number || '',  // From row
        consignment_marks: row.consignment_marks || '',  // From row
        consignment_voyage: row.consignment_voyage || '',  // From row
        containers: parsedContainers,
        shippingDetails,  // Array of shipping details with assignedQty fallback
      };
      return formattedRow;
    });

    // Await all receivers (since map is async due to calculateETA)
    receivers = await Promise.all(receivers);

    // Parse attachments (from orders; aligned with create's attachmentsJson)
    let parsedAttachments = orderRow.attachments || [];
    if (typeof orderRow.attachments === 'string') {
      if (orderRow.attachments.trim() === '') {
        parsedAttachments = [];
      } else {
        try {
          parsedAttachments = JSON.parse(orderRow.attachments);
        } catch (parseErr) {
          console.warn('Invalid JSON in attachments for order', id, '- treating as single path');
          parsedAttachments = [orderRow.attachments];
        }
      }
    }

    // Parse gatepass (from transport_details via t.gatepass; aligned with create's gatepassJson)
    let parsedGatepass = orderRow.gatepass || [];
    if (typeof orderRow.gatepass === 'string') {
      if (orderRow.gatepass.trim() === '') {
        parsedGatepass = [];
      } else {
        try {
          parsedGatepass = JSON.parse(orderRow.gatepass);
        } catch (parseErr) {
          console.warn('Invalid JSON in gatepass for order', id, '- treating as single path');
          parsedGatepass = [orderRow.gatepass];
        }
      }
    }

    // Format dates to YYYY-MM-DD for frontend (from transport_details; aligned with create)
    const formattedOrderRow = {
      ...orderRow,
      drop_date: orderRow.drop_date ? new Date(orderRow.drop_date).toISOString().split('T')[0] : '',
      delivery_date: orderRow.delivery_date ? new Date(orderRow.delivery_date).toISOString().split('T')[0] : ''
    };

    // Derive overall order status based on receivers' statuses (aligned with create's per-receiver status)
    let overallStatus = 'Created'; // Default if no receivers
    if (receivers.length > 0) {
      const receiverStatuses = receivers.map(r => r.status || 'Created');
      if (receiverStatuses.includes('Cancelled')) {
        overallStatus = 'Cancelled';  // Override: if any cancelled, whole order is
      } else {
        const statusOrder = { 'Created': 0, 'In Process': 1, 'Ready for Loading': 2, 'Loaded into Container': 3, 'Delivered': 4 };  // Extended for your statuses
        const maxStatusIndex = Math.max(...receiverStatuses.map(s => statusOrder[s] || 0));
        overallStatus = Object.keys(statusOrder).find(key => statusOrder[key] === maxStatusIndex) || 'Created';
      }
    }

    // Derive overall eta: min eta from receivers with containers assigned (aligned with per-receiver logic)
    let overallEta = null;
    const receiversWithContainers = receivers.filter(r => r.containers && r.containers.length > 0);
    if (receiversWithContainers.length > 0) {
      const etas = receiversWithContainers
        .map(r => r.eta)
        .filter(eta => eta)  // Non-null
        .map(eta => new Date(eta).getTime())
        .sort((a, b) => a - b);  // Earliest first
      overallEta = etas.length > 0 ? new Date(etas[0]).toISOString().split('T')[0] : null;
    } else {
      console.log(`[getOrderById ${id}] No receivers with containers; overall eta hidden`);
    }

    const orderData = {
      ...formattedOrderRow,
      eta: overallEta,  // Derived overall eta (only if any receiver has containers)
      overall_status: overallStatus, // New field for derived status
      status: overallStatus, // Override for backward compatibility
      attachments: Array.isArray(parsedAttachments) ? parsedAttachments : [],
      gatepass: Array.isArray(parsedGatepass) ? parsedGatepass : [],
      collection_scope: orderRow.collection_scope,
      qty_delivered: orderRow.qty_delivered,
      receivers,  // With conditional eta/etd (only if containers assigned), parsed containers, nested shippingDetails array including assignedQty fallback
      color: getOrderStatusColor(overallStatus)  // Assumes this function is defined elsewhere
    };

    console.log(`[getOrderById ${id}] Final response structure: receivers=${orderData.receivers.length}, overall_eta=${overallEta || 'HIDDEN (No Containers)'}, total_assigned_qty=${orderData.total_assigned_qty || 0}`);

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
  } finally {
    if (client) {
      client.release();
    }
  }
}
  
export async function assignContainersToOrders(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    const updates = req.body || {};
    const created_by = 'system'; // Or from auth
    // Map incoming snake_case to camelCase for consistency (for fixed fields)
    const camelUpdates = {
      orderIds: updates.order_ids || updates.orderIds,
      containerId: updates.container_id || updates.containerId,
      // Dynamic assignments object remains as-is
    };
    // Extract orderIds and assignments
    let orderIds = camelUpdates.orderIds || Object.keys(updates).filter(k => !isNaN(parseInt(k)));
    let assignments = camelUpdates.containerId || updates; // Fallback to full body if nested
    if (Array.isArray(orderIds)) {
      orderIds = orderIds.filter(id => !isNaN(parseInt(id)));
    } else {
      orderIds = Array.isArray(orderIds) ? orderIds : [];
    }
    // Debug log
    console.log('Assign containers body (key fields):', updates, {
      orderIds: orderIds.slice(0, 3), // Sample for log
      numOrders: orderIds.length,
      sampleAssignment: assignments && Object.keys(assignments).length > 0 ? { [Object.keys(assignments)[0]]: Object.keys(assignments[Object.keys(assignments)[0]] || {}).slice(0, 1) } : null
    });
    // Validation
    const updateErrors = [];
    if (orderIds.length === 0) {
      updateErrors.push('orderIds array is required and must not be empty');
    }
    if (!assignments || typeof assignments !== 'object' || Object.keys(assignments).length === 0) {
      updateErrors.push('assignments object is required and must not be empty');
    }
    if (updateErrors.length > 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({
        error: 'Invalid request fields',
        details: updateErrors.join('; ')
      });
    }
    // Collect all unique container IDs from the assignments
    const allCids = new Set();
    for (const orderAssign of Object.values(assignments)) {
      if (typeof orderAssign === 'object') {
        for (const recAssign of Object.values(orderAssign)) {
          if (typeof recAssign === 'object' && recAssign !== null) {
            for (const detailAssign of Object.values(recAssign)) {
              if (detailAssign && typeof detailAssign === 'object' && Array.isArray(detailAssign.containers)) {
                detailAssign.containers.forEach(cidStr => {
                  const cid = parseInt(cidStr);
                  if (!isNaN(cid)) {
                    allCids.add(cid);
                  }
                });
              }
            }
          }
        }
      }
    }
    if (allCids.size === 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'No valid containers specified in assignments' });
    }
    // Fetch container details (numbers) for all unique cids
    // Fetch container details (numbers) for all unique cids
const containerQuery = `
  SELECT 
    cm.cid, 
    cm.container_number,
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
    END as derived_status
  FROM container_master cm
  LEFT JOIN LATERAL (
    SELECT location, availability
    FROM container_status css
    WHERE css.cid = cm.cid
    ORDER BY css.sid DESC NULLS LAST
    LIMIT 1
  ) cs ON true
  LEFT JOIN container_hire_details chd ON cm.cid = chd.cid
  WHERE cm.cid = ANY($1) AND (
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
  ) = 'Available'
`;
const containerResult = await client.query(containerQuery, [Array.from(allCids)]);
const containerMap = new Map(containerResult.rows.map(row => [row.cid, row.container_number]));
// Check for missing or unavailable containers
    const missingCids = Array.from(allCids).filter(cid => !containerMap.has(cid));
    if (missingCids.length > 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: `Containers not found or not available: ${missingCids.join(', ')}` });
    }
    // Track updates
    const updatedOrders = [];
    const trackingData = [];
    // MODIFICATION: Accumulate total assigned qty per order for final update
    const orderAssignedQtys = new Map();
    for (const orderIdStr of orderIds) {
      const orderId = parseInt(orderIdStr);
      if (isNaN(orderId)) {
        console.warn(`Invalid order_id: ${orderIdStr}`);
        continue;
      }
      if (!assignments[orderIdStr]) {
        console.warn(`No assignments for order: ${orderId}`);
        updatedOrders.push({ orderId, assigned: 0 });
        continue;
      }
      // Fetch order
      const orderQuery = `
        SELECT id, booking_ref, status as overall_status, total_assigned_qty 
        FROM orders 
        WHERE id = $1
      `;
      const orderResult = await client.query(orderQuery, [orderId]);
      if (orderResult.rowCount === 0) {
        console.warn(`Order not found: ${orderId}`);
        continue;
      }
      const order = orderResult.rows[0];
      let currentTotalAssigned = parseInt(order.total_assigned_qty) || 0;
      let assignedForThisOrder = 0; // Accumulate for this order
      let assignedCount = 0;
      const orderAssignments = assignments[orderIdStr];
      for (const recIdStr of Object.keys(orderAssignments)) {
        const recId = parseInt(recIdStr);
        if (isNaN(recId)) {
          console.warn(`Invalid receiver_id: ${recIdStr}`);
          continue;
        }
        // Fetch receiver (include eta, etd, status for calculation)
        const receiverQuery = `
          SELECT id, receiver_name, containers, qty_delivered, eta, etd, status 
          FROM receivers 
          WHERE id = $1 AND order_id = $2
        `;
        const receiverResult = await client.query(receiverQuery, [recId, orderId]);
        if (receiverResult.rowCount === 0) {
          console.warn(`Receiver not found: ${recId} for order ${orderId}`);
          continue;
        }
        const receiver = receiverResult.rows[0];
        // Parse current containers
        let currentContainers = [];
        if (receiver.containers && typeof receiver.containers === 'string') {
          try {
            currentContainers = JSON.parse(receiver.containers);
            if (!Array.isArray(currentContainers)) {
              currentContainers = [];
            }
          } catch (parseErr) {
            console.warn(`Failed to parse containers for receiver ${recId}:`, parseErr.message);
            currentContainers = [];
          }
        }
        // Collect new containers and sum qty for this receiver
        const newContNumbers = new Set();
        let sumQty = 0;
        const recAssignments = orderAssignments[recIdStr];
        for (const detailIdxStr of Object.keys(recAssignments)) {
          const detailAssign = recAssignments[detailIdxStr];
          if (detailAssign && typeof detailAssign === 'object') {
            sumQty += parseInt(detailAssign.qty) || 0;
            if (Array.isArray(detailAssign.containers)) {
              detailAssign.containers.forEach(cidStr => {
                const cid = parseInt(cidStr);
                if (!isNaN(cid) && containerMap.has(cid)) {
                  newContNumbers.add(containerMap.get(cid));
                }
              });
            }
          }
        }
        if (sumQty === 0 && newContNumbers.size === 0) {
          console.warn(`No qty or containers to assign for receiver ${recId}`);
          continue;
        }
        // Append unique new containers (now containers assigned, so calculate/set eta if needed)
        const allContainers = new Set([...currentContainers, ...newContNumbers]);
        const updatedContainersJson = JSON.stringify(Array.from(allContainers));
        // Dynamically assign eta/etd if containers now assigned (and eta null)
        let newEta = receiver.eta;
        let newEtd = receiver.etd;
        if (allContainers.size > 0 && (!newEta || newEta === null)) {
          // Calculate eta based on current receiver status
          newEta = await calculateETA(client, receiver.status || 'Created');
          console.log(`[assignContainers] Calculated new ETA for receiver ${recId}: ${newEta} (status: ${receiver.status})`);
        }
        // ETD: Similar logic if needed (e.g., set to today or calculate differently); here assuming null/unchanged
        // Update qty_delivered
        const newDelivered = (parseInt(receiver.qty_delivered) || 0) + sumQty;
        // Update receiver with eta/etd if changed
        const updateReceiverQuery = `
          UPDATE receivers 
          SET containers = $1, qty_delivered = $2, eta = COALESCE(eta, $3), etd = $4
          WHERE id = $5
          RETURNING id
        `;
        const updateResult = await client.query(updateReceiverQuery, [updatedContainersJson, newDelivered, newEta, newEtd, recId]);
        if (updateResult.rowCount > 0) {
          assignedCount++;
          assignedForThisOrder += sumQty; // Accumulate for order
          trackingData.push({
            receiverId: recId,
            receiverName: receiver.receiver_name,
            orderId: order.id,
            bookingRef: order.booking_ref,
            assignedQty: sumQty,
            assignedContainers: Array.from(newContNumbers),
            status: order.overall_status,
            newEta: newEta  // Include new eta in tracking for logs/response
          });
          console.log(`Assigned ${sumQty} qty and ${newContNumbers.size} containers to receiver ${recId} (order ${orderId}); ETA set to ${newEta}`);
        } else {
          console.warn(`No rows updated for receiver ${recId}`);
        }
      }
      // After processing all receivers for this order, update the order's total_assigned_qty
      if (assignedForThisOrder > 0) {
        const newTotalAssigned = currentTotalAssigned + assignedForThisOrder;
        orderAssignedQtys.set(orderId, newTotalAssigned);
        const updateOrderQuery = `
          UPDATE orders 
          SET total_assigned_qty = $1
          WHERE id = $2
          RETURNING id, total_assigned_qty
        `;
        const orderUpdateResult = await client.query(updateOrderQuery, [newTotalAssigned, orderId]);
        if (orderUpdateResult.rowCount === 0) {
          console.warn(`Failed to update total_assigned_qty for order ${orderId}`);
        } else {
          console.log(`Updated total_assigned_qty for order ${orderId} to ${newTotalAssigned}`);
        }
      }
      updatedOrders.push({ 
        orderId: order.id, 
        bookingRef: order.booking_ref, 
        assignedReceivers: assignedCount,
        assignedQty: assignedForThisOrder
      });
    }
    await client.query('COMMIT');
    res.status(200).json({ 
      success: true, 
      message: `Assigned containers to ${trackingData.length} receivers across ${updatedOrders.length} orders`,
      updatedOrders,
      tracking: trackingData 
    });
  } catch (error) {
    console.error('Error assigning containers:', error);
    if (client) {
      try {
        await client.query('ROLLBACK');
        console.log('Transaction rolled back successfully');
      } catch (rollbackErr) {
        console.error('Rollback failed:', rollbackErr);
      }
    }
    return res.status(500).json({ error: 'Internal server error', details: error.message });
  } finally {
    if (client) {
      client.release();
    }
  }
}
export async function updateReceiverStatus(req, res) {
  let client;
  try {
    const orderId = req.params.orderId;
    const receiverId = req.params.id;
    const { status, notifyClient = true, notifyParties = false } = req.body || {};

    console.log('Received request to update receiver status:', { orderId, receiverId }, { status, notifyClient, notifyParties });

    // Validation
    const validStatuses = [
      'Ready for Loading', 'Loaded Into container', 'Shipment Processing',
      'Shipment In Transit', 'Under Processing', 'Arrived at Sort Facility',
      'Ready for Delivery', 'Shipment Delivered'
    ];
    if (!orderId || isNaN(parseInt(orderId))) {
      return res.status(400).json({ error: 'Valid order ID is required' });
    }
    if (!receiverId || isNaN(parseInt(receiverId))) {
      return res.status(400).json({ error: 'Valid receiver ID is required' });
    }
    if (!status || !validStatuses.includes(status)) {
      return res.status(400).json({ error: 'Valid status is required', validStatuses });
    }

    client = await pool.connect();
    await client.query('BEGIN');

    // Fetch order and receiver details (for notifications and old status)
    const detailsQuery = `
      SELECT o.*, s.sender_email, s.sender_contact,
             r.id as receiver_id, r.receiver_name, r.receiver_email, r.receiver_contact, r.status as receiver_status
      FROM orders o
      LEFT JOIN senders s ON o.id = s.order_id
      LEFT JOIN receivers r ON o.id = r.order_id AND r.id = $2
      WHERE o.id = $1
    `;
    const detailsResult = await client.query(detailsQuery, [parseInt(orderId), parseInt(receiverId)]);
    if (detailsResult.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Order or Receiver not found' });
    }
    const order = detailsResult.rows[0];
    const oldStatus = order.receiver_status;

    // Update receiver status
    const updateQuery = `
      UPDATE receivers
      SET status = $1, updated_at = CURRENT_TIMESTAMP
      WHERE id = $2 AND order_id = $3
      RETURNING id, status
    `;
    const updateResult = await client.query(updateQuery, [status, parseInt(receiverId), parseInt(orderId)]);
    if (updateResult.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(500).json({ error: 'Failed to update receiver status' });
    }

    const updatedReceiver = updateResult.rows[0];

    // Cascade: Update related order_items' consignment_status
    await client.query(`
      UPDATE order_items 
      SET consignment_status = $1, updated_at = CURRENT_TIMESTAMP 
      WHERE receiver_id = $2
    `, [status, parseInt(receiverId)]);

    // Cascade: Update overall order status (enhanced with propagation)
    await updateOrderOverallStatus(client, parseInt(orderId), status);

    // Cascade: To linked containers (if any) - Safe implementation to avoid "relation containers does not exist"
    try {
      const recQuery = `
        SELECT containers FROM receivers WHERE order_id = $1 AND id = $2
      `;
      const recResult = await client.query(recQuery, [parseInt(orderId), parseInt(receiverId)]);
      if (recResult.rowCount > 0 && recResult.rows[0].containers) {
        let contNums = [];
        const containersJson = recResult.rows[0].containers;
        if (typeof containersJson === 'string') {
          try {
            contNums = JSON.parse(containersJson);
          } catch (e) {
            if (containersJson.trim() && containersJson.trim() !== '[]') {
              contNums = [containersJson.trim()];
            }
          }
        } else if (Array.isArray(containersJson)) {
          contNums = containersJson;
        }
        if (contNums.length > 0) {
          // Map receiver status to container derived_status
          const statusMap = {
            'Ready for Loading': 'Ready for Loading',
            'Loaded Into container': 'Loaded',
            'Shipment Processing': 'In Transit',
            'Shipment In Transit': 'In Transit',
            'Under Processing': 'Under Processing',
            'Arrived at Sort Facility': 'Arrived',
            'Ready for Delivery': 'Ready for Delivery',
            'Shipment Delivered': 'Delivered'
          };
          const containerStatus = statusMap[status] || 'In Transit';
          // Update each matching container in container_master
          for (const contNum of contNums) {
            await client.query(
              'UPDATE container_master SET derived_status = $1 WHERE container_number = $2',
              [containerStatus, contNum]
            );
          }
          console.log(`Cascaded status "${containerStatus}" to ${contNums.length} containers for receiver ${receiverId}`);
        }
      }
    } catch (cascadeErr) {
      console.warn('Cascade to containers failed (non-critical):', cascadeErr.message);
      // Continue without failing the transaction
    }

    // Log to order_tracking with old_status
    await client.query(`
      INSERT INTO order_tracking (order_id, receiver_id, status, old_status, created_by, created_time)
      VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
    `, [parseInt(orderId), parseInt(receiverId), status, oldStatus, req.user?.id || 'system']);

    await client.query('COMMIT');

    // Trigger notifications (GAS-style: client/parties + subscribers)
    await triggerNotifications(order, status, notifyClient, notifyParties, { receiver: updatedReceiver, oldStatus });
    await sendUpdateToSubscribers(orderId, status, oldStatus); // New: Send to subscribers

    res.status(200).json({  
      success: true,
      message: `Receiver status updated to "${status}". Cascades and notifications triggered.`,
      updatedReceiver: { id: updatedReceiver.id, status: updatedReceiver.status }
    });
  } catch (error) {
    console.error('Error updating receiver status:', error);
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackErr) {
        console.error('Rollback failed:', rollbackErr);
      }
    }
    return res.status(500).json({ error: 'Internal server error', details: error.message });
  } finally {
    if (client) client.release();
  }
}
async function updateOrderOverallStatus(client, orderId, newReceiverStatus) {
  try {
    // Fetch all receivers
    const receiversQuery = `SELECT status FROM receivers WHERE order_id = $1`;
    const receiversResult = await client.query(receiversQuery, [orderId]);
    const receiverStatuses = receiversResult.rows.map(r => r.status);

    // Logic: Propagate if all match newReceiverStatus; else derive
    let overallStatus = 'In Process'; // Default
    if (receiverStatuses.every(s => s === newReceiverStatus)) {
      overallStatus = newReceiverStatus; // Full cascade to order
    } else if (receiverStatuses.every(s => s === 'Delivered')) {
      overallStatus = 'Delivered';
    } else if (receiverStatuses.some(s => s === 'Cancelled')) {
      overallStatus = 'Cancelled';
    } else if (receiverStatuses.some(s => s === 'Hold')) {
      overallStatus = 'Hold';
    } // Extend with more GAS-like phases (e.g., 'Shipment In Transit')

    const orderUpdateQuery = `
      UPDATE orders SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2 RETURNING id, status
    `;
    const orderResult = await client.query(orderUpdateQuery, [overallStatus, orderId]);
    if (orderResult.rowCount > 0) {
      console.log(`Cascaded order status to: ${overallStatus} for order ${orderId}`);
    }
  } catch (err) {
    console.error('Error updating overall order status:', err);
  }
}
// Updated cascadeToContainers function - fetches from separate 'receivers' table (no orders.receivers column needed)
// Collects containers from all relevant receivers for the order
// Add/update this in your order.controller.js
async function cascadeToContainers(client, orderId, status, receiverId) {
  const CONTAINER_TABLE = 'container_master';  // Your table (singular)
  
  try {
    // Step 1: Fetch containers from receivers table for the order (specific receiver or all)
    const fetchQuery = `
      SELECT id, containers
      FROM receivers
      WHERE order_id = $1 
        AND (id = $2 OR $2 IS NULL)
    `;
    const fetchParams = [orderId, receiverId];
    
    const fetchResult = await client.query(fetchQuery, fetchParams);
    
    if (fetchResult.rowCount === 0) {
      console.log(`No receivers found for order ${orderId}, receiver ${receiverId || 'all'}`);
      return;
    }

    const allContainerNumbers = new Set();  // Dedupe across receivers

    fetchResult.rows.forEach(row => {
      // Parse receiver's containers array (assumes JSON array like ["SLUO1234521"])
      if (row.containers) {
        let containersArray;
        if (typeof row.containers === 'string') {
          // Fallback: If stored as CSV string, split it
          containersArray = row.containers.split(',');
        } else if (Array.isArray(row.containers)) {
          containersArray = row.containers;
        } else {
          console.warn(`Unexpected containers format for receiver ${row.id}:`, typeof row.containers);
          return;
        }
        containersArray.forEach(cn => {
          const trimmed = (cn || '').toString().trim();
          if (trimmed) allContainerNumbers.add(trimmed);
        });
      }
    });

    const containerNumbers = Array.from(allContainerNumbers);
    if (containerNumbers.length === 0) {
      console.log(`No valid container numbers found for order ${orderId}, receiver ${receiverId || 'all'}`);
      return;
    }

    console.log(`Found containers to cascade from receivers table: [${containerNumbers.join(', ')}]`);

    // Step 2: Derive status (direct map; customize if needed)
    let derivedStatus = status;

    // Step 3: Dynamic UPDATE on container_master
    const placeholders = containerNumbers.map((_, i) => `$${i + 2}`).join(', ');  // Starts after $1
    const updateQuery = `
      UPDATE ${CONTAINER_TABLE}
      SET 
        derived_status = $1,
        updated_at = CURRENT_TIMESTAMP
      WHERE container_number IN (${placeholders})
    `;
    const updateParams = [derivedStatus, ...containerNumbers];

    const updateResult = await client.query(updateQuery, updateParams);
    
    console.log(`✅ Cascaded "${derivedStatus}" to ${updateResult.rowCount} containers in ${CONTAINER_TABLE} for order ${orderId}, receiver ${receiverId || 'all'}.`);
    
  } catch (err) {
    console.error(`❌ Cascade to ${CONTAINER_TABLE} failed:`, err.message);
    if (err.code === '42703') {  // Column does not exist
      console.error(`Column error - Verify: 'containers' column exists in 'receivers' table.`);
      console.error(`Sample query for debug: SELECT id, containers FROM receivers WHERE order_id = ${orderId};`);
    } else if (err.code === '42P01') {
      console.error(`Table ${CONTAINER_TABLE} or 'receivers' missing.`);
    }
    throw err;  // Isolate in main tx
  }
}
async function calculateETA(client, status, baseDate = new Date()) {
  try {
    const configQuery = `SELECT days_offset FROM eta_config WHERE status = $1`;
    const configResult = await client.query(configQuery, [status]);
    if (configResult.rowCount === 0) {
      console.log(`No ETA config for status: ${status}; using baseDate`);
      return baseDate.toISOString().split('T')[0]; // YYYY-MM-DD
    }
    const days = configResult.rows[0].days_offset;
    if (status.toLowerCase().includes('delivered')) return baseDate.toISOString().split('T')[0]; // No offset
    const etaDate = new Date(baseDate.getTime() + days * 86400000); // ms per day
    return etaDate.toISOString().split('T')[0];
  } catch (err) {
    console.error('ETA calc error:', err);
    return new Date().toISOString().split('T')[0];
  }
}

// Updated sendUpdateToSubscribers - fetches order/subscriber details, uses enhanced email with full template
async function sendUpdateToSubscribers(orderId, newStatus, oldStatus) {
  console.log(`Preparing to send update to subscribers for order ${orderId}: ${oldStatus} -> ${newStatus}`);  
  try {
    // Fetch order details (adapted for your schema; assumes 'reference_id' in receivers or orders)
    const orderQuery = `
      SELECT o.booking_ref, o.eta, o.updated_at as last_updated, o.sender_name, o.receiver_name, o.receiver_item_ref
      FROM orders o
      LEFT JOIN receivers r ON o.id = r.order_id
      WHERE o.id = $1
    `;
    const orderResult = await pool.query(orderQuery, [orderId]);
    if (orderResult.rowCount === 0) {
      console.log(`No order found for ID ${orderId}`);
      return;
    }

    const order = orderResult.rows[0];
    const tz = 'Asia/Dubai'; // RGSL timezone
    const now = new Date().toLocaleString('en-US', { timeZone: tz });
    const etaFormatted = order.eta ? new Date(order.eta).toLocaleDateString('en-GB') : '—'; // dd/MM/yyyy
    const route = `${order.sender_name || ''} to ${order.receiver_name || ''}`; // Full route from sender/receiver

    // Fetch subscribers from notifications table (assuming it has order_id, reference_id, email)
    const subQuery = `SELECT email FROM notifications WHERE order_id = $1 AND reference_id = $2`;
    const subResult = await pool.query(subQuery, [orderId, order.reference_id]);
    const subscribers = subResult.rows.map(row => row.email).filter(email => email && email.includes('@'));
console.log(`Found ${orderId} subscribers for order ${orderId}:`, order);
    // if (subscribers.length === 0) {
    //   console.log(`No subscribers for order ${orderId}`);
    //   return;
    // }

    // Get phase details (assume getPhase function exists; fallback if not)
    // const phase = getPhase ? getPhase(newStatus) : { label: newStatus, msg: `Updated from "${oldStatus}" to "${newStatus}".` };

    // Template data for shipment update
    const shipmentData = {
      statusLabel: '' || `Status: ${newStatus}`,
      statusMsg: 'phase.msg' || `Updated from "${oldStatus}" to "${newStatus}".`,
      refId: order.reference_id || order.booking_ref || '—',
      orderId: order.booking_ref || '—',
      route: route,
      etaFormatted,
      lastUpdated: now,
      trackLink: `https://ordertracking.royalgulfshipping.com/?ref=${encodeURIComponent(order.reference_id || order.booking_ref)}`
    };
       const email='support2@royalgulfshipping.com'; // For testing, send to fixed email
    // Send to each subscriber (uses updated sendShipmentEmail)
    // for (const email of subscribers) {
      await sendShipmentEmail(email, shipmentData);
      console.log(`Update email sent to ${email} for order ${orderId}: ${newStatus}`);
    // }
  } catch (err) {
    console.error('Subscriber email error:', err);
  }
}


async function sendShipmentEmail(email='support2@royalgulfshipping.com', shipmentData) {
  // Prepare data for the shipment update template
  const templateData = {
    statusLabel: shipmentData.statusLabel,
    statusMsg: shipmentData.statusMsg,
    refId: shipmentData.refId,
    orderId: shipmentData.orderId,
    route: shipmentData.route,
    etaFormatted: shipmentData.etaFormatted,
    lastUpdated: shipmentData.lastUpdated,
    trackLink: shipmentData.trackLink
  };

  const subject = `Royal Gulf Shipping – ${shipmentData.statusLabel} (Ref: ${shipmentData.refId})`;

  try {
    const result = await sendOrderEmail([email], subject, templateData);  // Single recipient array
    if (result.success) {
      console.log(`Shipment email sent to ${email} (Message ID: ${result.messageId})`);
    } else {
      console.error(`Failed to send to ${email}: ${result.error}`);
    }
  } catch (err) {
    console.error(`Shipment email error for ${email}:`, err.message);
  }
}
async function triggerNotifications(order, status, notifyClient, notifyParties) {
  const { booking_ref, sender_email, sender_contact, receiver_email, receiver_contact } = order;
  const clientEmail = 'support2@royalgulfshipping.com'; // Assume from order or auth

  // Mapping: Which statuses trigger what
  const notificationRules = {
    'Received for Shipment': { client: true, parties: false, message: 'Order received and in process.' },
    'Waiting for Authentication': { client: true, parties: true, message: 'Please authenticate shipment. Click to verify.' },
    'Shipper Authentication Confirmed': { client: true, parties: true, message: 'Shipper confirmed. Awaiting consignee.' },
    'Waiting for Consignee Authentication': { client: true, parties: true, message: 'Receiver authentication needed.' },
    'Waiting for Shipper Authentication (if applicable)': { client: true, parties: true, message: 'Shipper re-authentication required.' },
    'Consignee Authentication Confirmed': { client: true, parties: true, message: 'Consignee confirmed. Proceeding.' },
    'In Process': { client: true, parties: false, message: 'Shipment processing complete. Ready for next steps.' },
    'Ready for Loading': { client: true, parties: false, message: 'Shipment ready for container loading.' },
    'Loaded into Container': { client: true, parties: false, message: 'Loaded into container.' },
    'Departed for Port': { client: true, parties: false, message: 'Vessel sailed from Karachi.' },
    'Offloaded at Port': { client: true, parties: false, message: 'Arrived and offloaded at Dubai port.' },
    'Clearance Completed': { client: true, parties: false, message: 'Customs cleared. Ready for collection.' },
    'Hold': { client: true, parties: false, message: 'Shipment on hold. Contact support.' },
    'Cancelled': { client: true, parties: true, message: 'Shipment cancelled.' },
    'Delivered': { client: true, parties: true, message: 'Shipment delivered successfully!' }
    // 'Containers Returned (Internal only)': No notification
  };

  const rule = notificationRules[status];
  if (!rule) return;

  const baseMessage = `${rule.message} Order: ${booking_ref}.`;
  const authLink = `https://portal.royalgulf.com/auth/${order.id}`; // Dynamic link

  if (notifyClient && rule.client) {
    await sendEmail(clientEmail, `Status Update: ${status}`, `${baseMessage} ${status.includes('Authentication') ? `Auth link: ${authLink}` : ''}`);
  }

  if (notifyParties && rule.parties) {
    // Sender
    if (sender_email) await sendEmail(sender_email, `Action Required: ${status}`, `${baseMessage} ${authLink}`);
    if (sender_contact) await sendSMS(sender_contact, baseMessage); // Pseudo SMS

    // Receiver
    if (receiver_email) await sendEmail(receiver_email, `Action Required: ${status}`, `${baseMessage} ${authLink}`);
    if (receiver_contact) await sendSMS(receiver_contact, baseMessage);
  }
}

// Pseudo functions (implement with nodemailer/twilio)
async function sendEmail(to, subject, body) {
  // e.g., transporter.sendMail({ to, subject, html: body });
  console.log(`Email sent to ${to}: ${subject} - ${body}`);
}

async function sendSMS(to, message) {
  // e.g., client.messages.create({ body: message, from: '+123', to });
  console.log(`SMS sent to ${to}: ${message}`);
}

// Helper: Auto-transitions (e.g., auth complete → In Process)
async function handleAutoTransitions(client, orderId, newStatus) {
  // Example: If both auth confirmed, set to 'In Process'
  const authCheckQuery = `
    SELECT COUNT(*) as auth_count
    FROM receivers r
    WHERE r.order_id = $1 AND r.status IN ('Shipper Authentication Confirmed', 'Consignee Authentication Confirmed')
    GROUP BY r.order_id
    HAVING COUNT(*) >= 2  -- Assume 2 parties
  `;
  // If conditions met, update
  // await client.query('UPDATE orders SET status = \'In Process\' WHERE id = $1', [orderId]);
  // Extend for other rules (e.g., cron for reminders)
}

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
    const { page = 1, limit = 10, status, booking_ref, container_id } = req.query;

    let whereClause = 'WHERE 1=1';
    let params = [];

    if (status) {
      whereClause += ' AND o.status = $' + (params.length + 1);
      params.push(status);
    }

    if (booking_ref) {
      whereClause += ' AND o.booking_ref ILIKE $' + (params.length + 1);
      params.push(`%${booking_ref}%`);
    }

    let containerNumbers = [];  // To store looked-up container numbers

    if (container_id) {
      const containerIds = container_id.split(',').map(id => id.trim()).filter(Boolean);
      if (containerIds.length > 0) {
        // First, fetch container numbers for these CIDs
        const idArray = containerIds.map(id => parseInt(id));
        const containerQuery = {
          text: 'SELECT container_number FROM container_master WHERE cid = ANY($1::int[])',
          values: [idArray]
        };
        const containerResult = await pool.query(containerQuery);
        containerNumbers = containerResult.rows.map(row => row.container_number).filter(Boolean);

        if (containerNumbers.length === 0) {
          // No containers found, early return empty
          return res.json({
            data: [],
            pagination: {
              page: parseInt(page),
              limit: parseInt(limit),
              total: 0,
              totalPages: 0
            }
          });
        }

        // Conditions for ot.container_id (exact numeric match on CIDs)
        const otConditions = containerIds.map((idStr) => {
          const paramIdx = params.length + 1;
          params.push(parseInt(idStr));
          return `ot.container_id = $${paramIdx}`;
        }).join(' OR ');

        // Conditions for cm.container_number (partial ILIKE on looked-up numbers)
        const cmConditions = containerNumbers.map((num) => {
          const paramIdx = params.length + 1;
          params.push(`%${num}%`);
          return `cm.container_number ILIKE $${paramIdx}`;
        }).join(' OR ');

        // Conditions for receivers JSONB (partial ILIKE on looked-up numbers in unnested elements)
        const receiverExists = containerNumbers.map((num) => {
          const paramIdx = params.length + 1;
          params.push(`%${num}%`);
          return `EXISTS (
            SELECT 1 FROM jsonb_array_elements_text(r.containers) AS cont 
            WHERE cont ILIKE $${paramIdx}
          )`;
        }).join(' OR ');

        whereClause += ` AND (
          (${otConditions}) OR
          (${cmConditions}) OR
          EXISTS (
            SELECT 1 FROM receivers r 
            WHERE r.order_id = o.id 
            AND r.containers IS NOT NULL 
            AND (${receiverExists})
          )
        )`;
      }
    }

    // Build SELECT fields dynamically (aligned with schema)
    let selectFields = [
      'o.*',  // Core orders
      's.sender_name, s.sender_contact, s.sender_address, s.sender_email, s.sender_ref, s.sender_type, s.selected_sender_owner',  // From senders
      't.transport_type, t.third_party_transport, t.driver_name, t.driver_contact, t.driver_nic',  // From transport_details
      't.driver_pickup_location, t.truck_number, t.drop_method, t.dropoff_name, t.drop_off_cnic',
      't.drop_off_mobile, t.plate_no, t.drop_date, t.collection_method, t.collection_scope, t.qty_delivered',  // From transport_details
      't.client_receiver_name, t.client_receiver_id, t.client_receiver_mobile, t.delivery_date',
      't.gatepass',  // From transport_details
      'ot.status AS tracking_status, ot.created_time AS tracking_created_time',  // Latest tracking
      'ot.container_id',  // Explicit for join
      'cm.container_number',  // From container_master
      // Fixed subquery for aggregated containers from all receivers (comma-separated unique container numbers)
      'COALESCE((SELECT string_agg(DISTINCT elem, \', \') FROM (SELECT jsonb_array_elements_text(r3.containers) AS elem FROM receivers r3 WHERE r3.order_id = o.id AND r3.containers IS NOT NULL AND jsonb_array_length(r3.containers) > 0) AS unnested), \'\') AS receiver_containers_json',
      // Subquery for full receivers as JSON array per order
      '(SELECT COALESCE(json_agg(row_to_json(r2)), \'[]\') FROM receivers r2 WHERE r2.order_id = o.id) AS receivers'
    ].join(', ');

    // Build joins as array for easier extension (removed receivers join, now in subquery)
    let joinsArray = [
      'LEFT JOIN senders s ON o.id = s.order_id',
      'LEFT JOIN transport_details t ON o.id = t.order_id',
      'LEFT JOIN LATERAL (SELECT ot2.status, ot2.created_time, ot2.container_id FROM order_tracking ot2 WHERE ot2.order_id = o.id ORDER BY ot2.created_time DESC LIMIT 1) ot ON true',  // Latest tracking
      'LEFT JOIN container_master cm ON ot.container_id = cm.cid'  // Join to container_master on cid
    ];

    const joins = joinsArray.join('\n      ');

    // For count, no need for subqueries or receivers join, but conditions on ot/cm/r are handled via the whereClause (which includes subqueries for r)
    const countQuery = `
      SELECT COUNT(DISTINCT o.id) as total_count
      FROM orders o
      ${joins}
      ${whereClause}
    `;

    // Main query (no GROUP BY needed now with subqueries)
    const query = `
      SELECT ${selectFields}
      FROM orders o
      ${joins}
      ${whereClause}
      ORDER BY o.created_at DESC
      LIMIT $${params.length + 1} OFFSET $${params.length + 2}
    `;

    params.push(parseInt(limit), (parseInt(page) - 1) * parseInt(limit));

    const [result, countResult] = await Promise.all([
      pool.query(query, params),
      pool.query(countQuery, params.slice(0, -2))  // without limit offset
    ]);

    const total = parseInt(countResult.rows[0].total_count);

    res.json({
      data: result.rows,
      pagination: {
        page: parseInt(page),
        limit: parseInt(limit),
        total,
        totalPages: Math.ceil(total / limit)
      }
    });

  } catch (err) {
    console.error("Error fetching orders:", err);
    if (err.code === '42703') {
      return res.status(500).json({ error: 'Database schema mismatch. Check table/column names in query.' });
    }
    res.status(500).json({ error: 'Failed to fetch orders', details: err.message });
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
