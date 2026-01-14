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
// function getOrderStatusColor(status) {
//   const colors = {
//     'Created': 'info',
//     'In Transit': 'warning',
//     'Delivered': 'success',
//     'Cancelled': 'error'
//   };
//   return colors[status] || 'default';
// }

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
      dropOffDetails: updates.drop_off_details,  // NEW: Parse flattened drop-off details
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
      order_items_sample: camelUpdates.order_items ? JSON.parse(camelUpdates.order_items).slice(0,1) : null,
      dropOffDetails_sample: camelUpdates.dropOffDetails ? JSON.parse(camelUpdates.dropOffDetails).slice(0,1) : null  // NEW: Debug drop-off
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
        if (item.item_ref || item.itemRef) {
          const itemRef = item.item_ref || item.itemRef;
          const parts = itemRef.split('-');
          if (parts.length >= 6) {
            const partyIdx = parseInt(parts[3]) - 1;
            if (!isNaN(partyIdx) && partyIdx >= 0) {
              if (!acc[partyIdx]) acc[partyIdx] = [];
              acc[partyIdx].push(item);
            }
          } else {
            // MODIFICATION: Fallback - assign to first party (index 0) if item_ref is invalid/short
            console.warn(`Unparseable item_ref "${itemRef}" for item; assigning to party 0`);
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
        containers: party.containers || party.containerDetails || [], // Support receiver-level containers
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
        containers: party.containers || party.containerDetails || [], // Support receiver-level containers
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
      const totalNum = shippingDetails.reduce((sum, item) => sum + (parseInt(item.total_number || item.totalNumber || 0) || 0), 0);
      const totalWt = shippingDetails.reduce((sum, item) => sum + (parseFloat(item.weight || 0) || 0), 0);
      return {
        ...rec,
        total_number: totalNum > 0 ? totalNum : null,
        total_weight: totalWt > 0 ? totalWt : null,
      };
    });
    // NEW: Parse flattened drop-off details and group by receiver_index
    let parsedDropOffDetails = {};
    try {
      const flatDropOffs = JSON.parse(camelUpdates.dropOffDetails || '[]');
      flatDropOffs.forEach(dropOff => {
        const receiverIdx = dropOff.receiver_index;
        if (receiverIdx !== undefined && !isNaN(parseInt(receiverIdx))) {
          const idx = parseInt(receiverIdx);
          if (!parsedDropOffDetails[idx]) parsedDropOffDetails[idx] = [];
          // Map to snake_case for consistency
          parsedDropOffDetails[idx].push({
            drop_method: dropOff.dropMethod || '',
            dropoff_name: dropOff.dropoffName || '',
            drop_off_cnic: dropOff.dropOffCnic || '',
            drop_off_mobile: dropOff.dropOffMobile || '',
            plate_no: dropOff.plateNo || '',
            drop_date: dropOff.dropDate || ''
          });
        } else {
          console.warn(`Invalid receiver_index "${receiverIdx}" for drop-off; skipping`);
        }
      });
      console.log('Parsed drop-off details:', parsedDropOffDetails);
    } catch (e) {
      console.warn('Failed to parse drop_off_details:', e.message);
      parsedDropOffDetails = {};
    }
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
            // Handle camelCase fallbacks for UI consistency
            const pickupLoc = item.pickup_location || item.pickupLocation || '';
            const deliveryAddr = item.delivery_address || item.deliveryAddress || '';
            const category = item.category || '';
            const subcategory = item.subcategory || '';
            const type = item.type || '';
            if (!pickupLoc.trim()) console.warn(`pickup_location missing for shipping detail ${j + 1} of party ${index + 1}`);
            if (!category.trim()) console.warn(`category missing for shipping detail ${j + 1} of party ${index + 1}`);
            if (!subcategory.trim()) console.warn(`subcategory missing for shipping detail ${j + 1} of party ${index + 1}`);
            if (!type.trim()) console.warn(`type missing for shipping detail ${j + 1} of party ${index + 1}`);
            if (!deliveryAddr.trim()) console.warn(`delivery_address missing for shipping detail ${j + 1} of party ${index + 1}`);
            const num = parseInt(item.total_number || item.totalNumber || 0);
            if (num <= 0) console.warn(`total_number must be positive for shipping detail ${j + 1} of party ${index + 1}`);
            const wt = parseFloat(item.weight || 0);
            if (wt <= 0) console.warn(`weight must be positive for shipping detail ${j + 1} of party ${index + 1}`);
            // New: Validate containerDetails if provided
            const containerDetails = item.containerDetails || item.container_details || [];
            if (Array.isArray(containerDetails) && containerDetails.length > 0) {
              containerDetails.forEach((cont, k) => {
                if (!cont.container_number?.trim()) console.warn(`container_number missing for container ${k + 1} in detail ${j + 1} of party ${index + 1}`);
              });
            }
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
    // NEW: Validate drop-off details if provided (match to receivers)
    if (Object.keys(parsedDropOffDetails).length > 0) {
      Object.keys(parsedDropOffDetails).forEach(receiverIdxStr => {
        const idx = parseInt(receiverIdxStr);
        if (idx >= 0 && idx < parsedReceivers.length) {
          const dropOffs = parsedDropOffDetails[idx];
          dropOffs.forEach((dropOff, dIndex) => {
            if (!dropOff.drop_method?.trim()) console.warn(`drop_method missing for drop-off ${dIndex + 1} of receiver ${idx + 1}`);
            if (dropOff.drop_date) {
              try {
                new Date(dropOff.drop_date);  // Basic date validation
              } catch {
                console.warn(`Invalid drop_date format for drop-off ${dIndex + 1} of receiver ${idx + 1}`);
              }
            }
          });
        } else {
          updateErrors.push(`drop_off_details receiver_index ${receiverIdxStr} out of range (0-${parsedReceivers.length - 1})`);
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
      // FIXED: Extract eta string from calculateETA result (handles object return)
      let etaCalc = rec.eta ? normalizeDate(rec.eta) : await calculateETA(client, rec.status || 'Created');
      const recNormEta = typeof etaCalc === 'string' ? etaCalc : (etaCalc ? etaCalc.eta : null);
      const recNormEtd = rec.etd ? normalizeDate(rec.etd) : null;
      const receiversQuery = `
        INSERT INTO receivers (
          order_id, receiver_name, receiver_contact, receiver_address, receiver_email, eta, etd, shipping_line,
          consignment_vessel, consignment_number, consignment_marks, consignment_voyage,
          total_number, total_weight, remarks, containers, status, full_partial, qty_delivered
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
        RETURNING id, receiver_name
      `;
      const recContainersJson = JSON.stringify(rec.containers || []); // Updated to use parsed containers
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
      // Insert multiple order_items per receiver (now with status and containers per detail)
      for (let j = 0; j < shippingDetails.length; j++) {
        const item = shippingDetails[j];
        // Handle camelCase fallbacks for UI consistency
        const pickupLoc = item.pickup_location || item.pickupLocation || '';
        const deliveryAddr = item.delivery_address || item.deliveryAddress || '';
        const category = item.category || '';
        const subcategory = item.subcategory || '';
        const type = item.type || '';
        const totalNum = parseInt(item.total_number || item.totalNumber || 0);
        const weight = parseFloat(item.weight || 0);
        const itemRef = item.item_ref || item.itemRef || '';
        const containerDetails = item.containerDetails || item.container_details || [];
        const orderItemsQuery = `
          INSERT INTO order_items (
            order_id, receiver_id, item_ref, pickup_location, delivery_address, category, subcategory, type,
            total_number, weight, container_details
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        `;
        const orderItemsValues = [
          orderId,
          receiverId,
          itemRef,
          pickupLoc,
          deliveryAddr,
          category,
          subcategory,
          type,
          totalNum,
          weight,
          JSON.stringify(containerDetails) // New: Save containerDetails as JSON
        ];
        await client.query(orderItemsQuery, orderItemsValues);
      }
      // NEW: Insert drop-off details for this receiver if any
      const receiverDropOffs = parsedDropOffDetails[i] || [];
      for (let d = 0; d < receiverDropOffs.length; d++) {
        const dropOff = receiverDropOffs[d];
        const dropOffQuery = `
          INSERT INTO drop_off_details (
            order_id, receiver_id, drop_method, dropoff_name, drop_off_cnic, drop_off_mobile, plate_no, drop_date
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        `;
        const normDropDate = dropOff.drop_date ? normalizeDate(dropOff.drop_date) : null;
        const dropOffValues = [
          orderId,
          receiverId,
          dropOff.drop_method || null,
          dropOff.dropoff_name || null,
          dropOff.drop_off_cnic || null,
          dropOff.drop_off_mobile || null,
          dropOff.plate_no || null,
          normDropDate
        ];
        await client.query(dropOffQuery, dropOffValues);
      }
      let containerId = null;
      // Skip container lookup since not in UI
      trackingData.push({
        receiverId,
        status: rec.status || updatedFields.status,
        totalShippingDetails: shippingDetails.length,
        totalDropOffDetails: receiverDropOffs.length  // NEW: Track drop-offs
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
        if (item.item_ref || item.itemRef) {
          const itemRef = item.item_ref || item.itemRef;
          const parts = itemRef.split('-');
          if (parts.length >= 6) {
            const partyIdx = parseInt(parts[3]) - 1;
            if (!isNaN(partyIdx) && partyIdx >= 0) {
              if (!acc[partyIdx]) acc[partyIdx] = [];
              acc[partyIdx].push(item);
            }
          } else {
            // MODIFICATION: Fallback - assign to first party (index 0) if item_ref is invalid/short
            console.warn(`Unparseable item_ref "${itemRef}" for item; assigning to party 0`);
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
        containers: party.containers || party.containerDetails || [], // Support receiver-level containers
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
        containers: party.containers || party.containerDetails || [], // Support receiver-level containers
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
      const totalNum = shippingDetails.reduce((sum, item) => sum + (parseInt(item.total_number || item.totalNumber || 0) || 0), 0);
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
    if (parsedReceivers.length === 0) {
      updateErrors.push('At least one shipping party is required');
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
            // Handle camelCase fallbacks for UI consistency
            const pickupLoc = item.pickup_location || item.pickupLocation || '';
            const deliveryAddr = item.delivery_address || item.deliveryAddress || '';
            const category = item.category || '';
            const subcategory = item.subcategory || '';
            const type = item.type || '';
            if (!pickupLoc.trim()) console.warn(`pickup_location missing for shipping detail ${j + 1} of party ${index + 1}`);
            if (!category.trim()) console.warn(`category missing for shipping detail ${j + 1} of party ${index + 1}`);
            if (!subcategory.trim()) console.warn(`subcategory missing for shipping detail ${j + 1} of party ${index + 1}`);
            if (!type.trim()) console.warn(`type missing for shipping detail ${j + 1} of party ${index + 1}`);
            if (!deliveryAddr.trim()) console.warn(`delivery_address missing for shipping detail ${j + 1} of party ${index + 1}`);
            const num = parseInt(item.total_number || item.totalNumber || 0);
            if (num <= 0) console.warn(`total_number must be positive for shipping detail ${j + 1} of party ${index + 1}`);
            const wt = parseFloat(item.weight || 0);
            if (wt <= 0) console.warn(`weight must be positive for shipping detail ${j + 1} of party ${index + 1}`);
            // New: Validate containerDetails if provided
            const containerDetails = item.containerDetails || item.container_details || [];
            if (Array.isArray(containerDetails) && containerDetails.length > 0) {
              containerDetails.forEach((cont, k) => {
                if (!cont.container_number?.trim()) console.warn(`container_number missing for container ${k + 1} in detail ${j + 1} of party ${index + 1}`);
              });
            }
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
        const insertReceiversQuery = `
          INSERT INTO receivers (
            order_id, receiver_name, receiver_contact, receiver_address, receiver_email, eta, etd, shipping_line,
            consignment_vessel, consignment_number, consignment_marks, consignment_voyage,
            total_number, total_weight, remarks, containers, status, full_partial, qty_delivered
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
          RETURNING id, receiver_name
        `;
        const recContainersJson = JSON.stringify(rec.containers || []); // Updated to use parsed containers
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
        const recResult = await client.query(insertReceiversQuery, receiversValues);
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
            { key: 'containers', val: JSON.stringify(rec.containers || []) }, // New: Update containers
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
          // Handle camelCase fallbacks for UI consistency
          const pickupLoc = item.pickup_location || item.pickupLocation || '';
          const deliveryAddr = item.delivery_address || item.deliveryAddress || '';
          const category = item.category || '';
          const subcategory = item.subcategory || '';
          const type = item.type || '';
          const totalNum = parseInt(item.total_number || item.totalNumber || 0);
          const weight = parseFloat(item.weight || 0);
          const itemRef = item.item_ref || item.itemRef || '';
          const containerDetails = item.containerDetails || item.container_details || [];
          const orderItemsQuery = `
            INSERT INTO order_items (
              order_id, receiver_id, item_ref, pickup_location, delivery_address, category, subcategory, type,
              total_number, weight, container_details
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
          `;
          const orderItemsValues = [
            id,
            receiverId,
            itemRef,
            pickupLoc,
            deliveryAddr,
            category,
            subcategory,
            type,
            totalNum,
            weight,
            JSON.stringify(containerDetails) // New: Save containerDetails as JSON
          ];
          await client.query(orderItemsQuery, orderItemsValues);
        }
      } else if (camelUpdates.order_items) {
        // For non-replace, delete and re-insert items if order_items provided
        await client.query('DELETE FROM order_items WHERE order_id = $1 AND receiver_id = $2', [id, receiverId]);
        for (let j = 0; j < shippingDetails.length; j++) {
          const item = shippingDetails[j];
          // Handle camelCase fallbacks for UI consistency
          const pickupLoc = item.pickup_location || item.pickupLocation || '';
          const deliveryAddr = item.delivery_address || item.deliveryAddress || '';
          const category = item.category || '';
          const subcategory = item.subcategory || '';
          const type = item.type || '';
          const totalNum = parseInt(item.total_number || item.totalNumber || 0);
          const weight = parseFloat(item.weight || 0);
          const itemRef = item.item_ref || item.itemRef || '';
          const containerDetails = item.containerDetails || item.container_details || [];
          const orderItemsQuery = `
            INSERT INTO order_items (
              order_id, receiver_id, item_ref, pickup_location, delivery_address, category, subcategory, type,
              total_number, weight, container_details
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
          `;
          const orderItemsValues = [
            id,
            receiverId,
            itemRef,
            pickupLoc,
            deliveryAddr,
            category,
            subcategory,
            type,
            totalNum,
            weight,
            JSON.stringify(containerDetails) // New: Save containerDetails as JSON
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
    // After COMMIT
    const updatedOrderResult = await client.query('SELECT * FROM orders WHERE id = $1', [id]);
    const updatedSenderResult = await client.query('SELECT * FROM senders WHERE order_id = $1', [id]);
    // NEW: Fetch enhanced receivers
    const fetchReceiversQuery = `
      SELECT 
        r.*,
        COALESCE(
          (
            SELECT json_agg(
              json_build_object(
                'pickupLocation', oi.pickup_location,
                'deliveryAddress', oi.delivery_address,
                'category', oi.category,
                'subcategory', oi.subcategory,
                'type', oi.type,
                'totalNumber', oi.total_number,
                'weight', oi.weight,
                'itemRef', oi.item_ref,
                'containerDetails', COALESCE(oi.container_details, '[]'::jsonb)
              ) ORDER BY oi.id
            )
            FROM order_items oi 
            WHERE oi.receiver_id = r.id
          ), 
          '[]'::json
        ) AS shippingDetails
      FROM receivers r 
      WHERE r.order_id = $1 
      ORDER BY r.id
    `;
    const enhancedReceiversResult = await client.query(fetchReceiversQuery, [id]);
    const enhancedReceivers = enhancedReceiversResult.rows.map(row => ({
      ...row,
      shippingDetails: row.shippingDetails || [],
      containers: typeof row.containers === 'string' ? JSON.parse(row.containers) : (row.containers || [])
    }));
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
          -- Existing summaries...
          rs.receiver_summary,
          rss.receiver_status_summary,
          rc.receiver_containers_json,
          rt.total_items,
          rt.remaining_items,
          -- Enhanced: receivers_details with nested shippingDetails
          rd.receivers_details,
          -- NEW: LATERAL join for shipping details per receiver
          json_agg(
            json_build_object(
              'receivers', rd.receivers_details,  -- Existing receiver info
              'shippingDetails', LATERAL (
                SELECT json_agg(
                  json_build_object(
                    'pickupLocation', oi.pickup_location,
                    'deliveryAddress', oi.delivery_address,
                    'category', oi.category,
                    'subcategory', oi.subcategory,
                    'type', oi.type,
                    'totalNumber', oi.total_number,
                    'weight', oi.weight,
                    'itemRef', oi.item_ref,
                    'containerDetails', COALESCE(oi.container_details, '[]'::jsonb)
                  ) ORDER BY oi.id
                ) AS sd
                FROM order_items oi
                WHERE oi.receiver_id = ANY(
                  SELECT jsonb_array_elements_text(rd.receivers_details::jsonb ->> 'id')::int  -- Extract receiver IDs
                )
                GROUP BY oi.receiver_id
              ).sd
            )
          ) AS full_receivers_with_details
        FROM orders o
        -- ... (keep all your existing LEFT JOINs)
        LEFT JOIN LATERAL (
          SELECT json_agg(json_build_object(
            'id', r.id,
            'receiver_name', r.receiver_name,
            -- ... (keep existing fields)
            'shippingDetails', LATERAL (
              SELECT json_agg(
                json_build_object(
                  'pickupLocation', oi.pickup_location,
                  -- ... (all oi fields as above)
                  'containerDetails', COALESCE(oi.container_details, '[]'::jsonb)
                ) ORDER BY oi.id
              ) AS sd
              FROM order_items oi WHERE oi.receiver_id = r.id
            ).sd
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
      receivers: enhancedReceivers,
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
// export async function getOrders(req, res) {
//   try {
//     // FIXED: Sanitize page and limit early to avoid NaN in offset calc
//     const rawPage = req.query.page || '1';
//     const rawLimit = req.query.limit || '10';
//     const safePage = Math.max(1, parseInt(rawPage) || 1);
//     const safeLimit = Math.max(1, Math.min(100, parseInt(rawLimit) || 10)); // Clamp 1-100
//     const safeOffset = (parseInt(safePage) - 1) * safeLimit;
//     const { status, booking_ref, container_id } = req.query;
//     let whereClause = 'WHERE 1=1';
//     let params = [];
//     if (status) {
//       whereClause += ' AND o.status = $' + (params.length + 1);
//       params.push(status);
//     }
//     if (booking_ref) {
//       whereClause += ' AND o.booking_ref ILIKE $' + (params.length + 1);
//       params.push(`%${booking_ref}%`);
//     }
//     let containerNumbers = []; // To store looked-up container numbers
//     if (container_id) {
//       const containerIds = container_id.split(',').map(id => id.trim()).filter(Boolean);
//       if (containerIds.length > 0) {
//         // FIXED: Filter valid numeric IDs to avoid NaN/empty in array
//         const validIds = containerIds.filter(id => !isNaN(parseInt(id)));
//         if (validIds.length === 0) {
//           // No valid containers, early return empty
//           return res.json({
//             data: [],
//             pagination: {
//               page: safePage,
//               limit: safeLimit,
//               total: 0,
//               totalPages: 0
//             }
//           });
//         }
//         const idArray = validIds.map(id => parseInt(id)); // Now safe: all ints
//         const containerQuery = {
//           text: 'SELECT container_number FROM container_master WHERE cid = ANY($1::int[])',
//           values: [idArray]
//         };
//         const containerResult = await pool.query(containerQuery);
//         containerNumbers = containerResult.rows.map(row => row.container_number).filter(Boolean);
//         if (containerNumbers.length === 0) {
//           // No containers found, early return empty
//           return res.json({
//             data: [],
//             pagination: {
//               page: safePage,
//               limit: safeLimit,
//               total: 0,
//               totalPages: 0
//             }
//           });
//         }
//         // Conditions for ot.container_id (exact numeric match on CIDs)
//         const otConditions = validIds.map((idStr) => {
//           const paramIdx = params.length + 1;
//           params.push(parseInt(idStr));
//           return `ot.container_id = $${paramIdx}`;
//         }).join(' OR ');
//         // Conditions for cm.container_number (partial ILIKE on looked-up numbers)
//         const cmConditions = containerNumbers.map((num) => {
//           const paramIdx = params.length + 1;
//           params.push(`%${num}%`);
//           return `cm.container_number ILIKE $${paramIdx}`;
//         }).join(' OR ');
//         // FIXED: Conditions for receivers JSONB (add jsonb_typeof check for safety)
//         const receiverExists = containerNumbers.map((num) => {
//           const paramIdx = params.length + 1;
//           params.push(`%${num}%`);
//           return `EXISTS (
//             SELECT 1 FROM jsonb_array_elements_text(r.containers) AS cont
//             WHERE cont ILIKE $${paramIdx}
//           )`;
//         }).join(' OR ');
//         whereClause += ` AND (
//           (${otConditions}) OR
//           (${cmConditions}) OR
//           EXISTS (
//             SELECT 1 FROM receivers r
//             WHERE r.order_id = o.id
//             AND r.containers IS NOT NULL
//             AND jsonb_typeof(r.containers) = 'array'
//             AND (${receiverExists})
//           )
//         )`;
//       }
//     }
// let selectFields = [
//   'o.*', // Core orders
//   's.sender_name, s.sender_contact, s.sender_address, s.sender_email, s.sender_ref, s.sender_type, s.selected_sender_owner', // From senders
//   't.transport_type, t.third_party_transport, t.driver_name, t.driver_contact, t.driver_nic', // From transport_details
//   't.driver_pickup_location, t.truck_number, t.drop_method, t.dropoff_name, t.drop_off_cnic',
//   't.drop_off_mobile, t.plate_no, t.drop_date, t.collection_method, t.collection_scope, t.qty_delivered', // From transport_details
//   't.client_receiver_name, t.client_receiver_id, t.client_receiver_mobile, t.delivery_date',
//   't.gatepass', // From transport_details
//   'ot.status AS tracking_status, ot.created_time AS tracking_created_time', // Latest tracking
//   'ot.container_id', // Explicit for join
//   'cm.container_number', // From container_master
//   // FIXED: Subquery for aggregated containers from all receivers (add jsonb_typeof for safety)
//   'COALESCE((SELECT string_agg(DISTINCT elem, \', \') FROM (SELECT jsonb_array_elements_text(r3.containers) AS elem FROM receivers r3 WHERE r3.order_id = o.id AND r3.containers IS NOT NULL AND jsonb_typeof(r3.containers) = \'array\' AND jsonb_array_length(r3.containers) > 0) AS unnested), \'\') AS receiver_containers_json',
//   // Updated subquery for full receivers as JSON array per order, using LATERAL with fixed ORDER BY position
//   `(SELECT COALESCE(json_agg(r2_full ORDER BY r2_full.id), '[]') FROM (
//     SELECT 
//       r2.id, r2.order_id, r2.receiver_name, r2.receiver_contact, r2.receiver_address, r2.receiver_email,
//       r2.total_number, r2.total_weight, r2.receiver_ref, r2.remarks, r2.containers,
//       r2.status, r2.eta, r2.etd, r2.shipping_line, r2.consignment_vessel, r2.consignment_number,
//       r2.consignment_marks, r2.consignment_voyage, r2.full_partial, r2.qty_delivered,
//       sd_full.shippingDetails
//     FROM receivers r2
//     LEFT JOIN LATERAL (
//       SELECT json_agg(
//         json_build_object(
//           'id', oi.id,
//           'order_id', oi.order_id,
//           'sender_id', oi.sender_id,
//           'category', COALESCE(oi.category, ''),
//           'subcategory', COALESCE(oi.subcategory, ''),
//           'type', COALESCE(oi.type, ''),
//           'pickupLocation', COALESCE(oi.pickup_location, ''),
//           'deliveryAddress', COALESCE(oi.delivery_address, ''),
//           'totalNumber', COALESCE(oi.total_number, 0),
//           'weight', COALESCE(oi.weight, 0),
//           'totalWeight', COALESCE(oi.total_weight, 0),
//           'itemRef', COALESCE(oi.item_ref, ''),
//           'consignmentStatus', COALESCE(oi.consignment_status, ''),
//           'shippingLine', COALESCE(oi.shipping_line, ''),
//           'containerDetails', COALESCE(oi.container_details, '[]'::jsonb),
//           'remainingItems', (COALESCE(oi.total_number, 0) - COALESCE((SELECT SUM(
//             CASE 
//               WHEN (elem->>\'assign_total_box\') ~ \'^[0-9]+\$' 
//               THEN (elem->>\'assign_total_box\')::int 
//               ELSE 0 
//             END
//           ) FROM jsonb_array_elements(COALESCE(oi.container_details, \'[]\'::jsonb)) AS elem), 0))
//         ) ORDER BY oi.id
//       ) AS shippingDetails
//       FROM order_items oi
//       WHERE oi.receiver_id = r2.id
//     ) sd_full ON true
//     WHERE r2.order_id = o.id
//   ) r2_full) AS receivers`
// ].join(', ');
//     // Build joins as array for easier extension (removed receivers join, now in subquery)
//     let joinsArray = [
//       'LEFT JOIN senders s ON o.id = s.order_id',
//       'LEFT JOIN transport_details t ON o.id = t.order_id',
//       'LEFT JOIN LATERAL (SELECT ot2.status, ot2.created_time, ot2.container_id FROM order_tracking ot2 WHERE ot2.order_id = o.id ORDER BY ot2.created_time DESC LIMIT 1) ot ON true', // Latest tracking
//       'LEFT JOIN container_master cm ON ot.container_id = cm.cid' // Join to container_master on cid
//     ];
//     const joins = joinsArray.join('\n ');
//     // For count, no need for subqueries or receivers join, but conditions on ot/cm/r are handled via the whereClause (which includes subqueries for r)
//     const countQuery = `
//       SELECT COUNT(DISTINCT o.id) as total_count
//       FROM orders o
//       ${joins}
//       ${whereClause}
//     `;
//     // Main query (no GROUP BY needed now with subqueries)
//     const query = `
//       SELECT ${selectFields}
//       FROM orders o
//       ${joins}
//       ${whereClause}
//       ORDER BY o.created_at DESC
//       LIMIT $${params.length + 1} OFFSET $${params.length + 2}
//     `;
//     // const safeOffset = (parseInt(safePage) - 1) * safeLimit;
//     // FIXED: Ensure limit/offset are always valid ints (fallback if NaN)
//     params.push(safeLimit, safeOffset);
//     const [result, countResult] = await Promise.all([
//       pool.query(query, params),
//       pool.query(countQuery, params.slice(0, -2)) // without limit offset
//     ]);
//     const total = parseInt(countResult.rows[0].total_count || 0);
//     res.json({
//       data: result.rows,
//       pagination: {
//         page: safePage,
//         limit: safeLimit,
//         total,
//         totalPages: total === 0 ? 0 : Math.ceil(total / safeLimit)
//       }
//     });
//   } catch (err) {
//     console.error("Error fetching orders:", err);
//     if (err.code === '42703') {
//       return res.status(500).json({ error: 'Database schema mismatch. Check table/column names in query.' });
//     }
//     res.status(500).json({ error: 'Failed to fetch orders', details: err.message });
//   }
// }

export async function getOrders(req, res) {
  try {
    // Sanitize pagination
    const rawPage = req.query.page || '1';
    const rawLimit = req.query.limit || '10';
    const safePage = Math.max(1, parseInt(rawPage) || 1);
    const safeLimit = Math.max(1, Math.min(100, parseInt(rawLimit) || 10));
    const safeOffset = (safePage - 1) * safeLimit;

    const { status, booking_ref, container_id } = req.query;
    let whereClause = 'WHERE 1=1';
    let params = [];

    // Filters
    if (status) {
      whereClause += ` AND o.status = $${params.length + 1}`;
      params.push(status);
    }
    if (booking_ref) {
      whereClause += ` AND o.booking_ref ILIKE $${params.length + 1}`;
      params.push(`%${booking_ref}%`);
    }

    let containerNumbers = [];
    if (container_id) {
      const containerIds = container_id.split(',').map(id => id.trim()).filter(Boolean);
      const validIds = containerIds.filter(id => !isNaN(parseInt(id)));
      if (validIds.length === 0) {
        return res.json({ data: [], pagination: { page: safePage, limit: safeLimit, total: 0, totalPages: 0 } });
      }

      const idArray = validIds.map(id => parseInt(id));
      const containerResult = await pool.query(
        'SELECT container_number FROM container_master WHERE cid = ANY($1::int[])',
        [idArray]
      );
      containerNumbers = containerResult.rows.map(r => r.container_number).filter(Boolean);

      if (containerNumbers.length === 0) {
        return res.json({ data: [], pagination: { page: safePage, limit: safeLimit, total: 0, totalPages: 0 } });
      }

      const otConditions = validIds.map(id => {
        params.push(parseInt(id));
        return `ot.container_id = $${params.length}`;
      }).join(' OR ');

      const cmConditions = containerNumbers.map(num => {
        params.push(`%${num}%`);
        return `cm.container_number ILIKE $${params.length}`;
      }).join(' OR ');

      const receiverExists = containerNumbers.map(num => {
        params.push(`%${num}%`);
        return `EXISTS (
          SELECT 1 FROM jsonb_array_elements_text(r.containers) AS cont
          WHERE cont ILIKE $${params.length}
        )`;
      }).join(' OR ');

      whereClause += ` AND (
        (${otConditions}) OR
        (${cmConditions}) OR
        EXISTS (
          SELECT 1 FROM receivers r
          WHERE r.order_id = o.id
          AND r.containers IS NOT NULL
          AND jsonb_typeof(r.containers) = 'array'
          AND (${receiverExists})
        )
      )`;
    }

    const safeIntCast = (val) => `COALESCE(${val}, 0)`;
    const totalAssignedSub = `(SELECT COALESCE(SUM(assigned_qty), 0) FROM container_assignment_history WHERE detail_id = oi.id)`;

    const containerDetailsSub = `
      COALESCE((
        SELECT json_agg(
          json_build_object(
            'status', COALESCE(cs.derived_status, CASE WHEN ass.assigned_qty > 0 THEN 'Ready for Loading' ELSE 'Created' END),
            'container', json_build_object('cid', u.cid, 'container_number', COALESCE(cm.container_number, '')),
            'total_number', ${safeIntCast('oi.total_number')},
            'assign_weight', CASE 
              WHEN tot.total_ass > 0 AND ass.assigned_qty > 0 
              THEN ROUND((ass.assigned_qty::numeric / tot.total_ass::numeric * ${safeIntCast('oi.weight')} / 1000), 2)::text 
              ELSE '0' 
            END,
            'remaining_items', (${safeIntCast('oi.total_number')} - COALESCE(tot.total_ass, 0))::text,
            'assign_total_box', COALESCE(ass.assigned_qty, 0)::text
          ) ORDER BY COALESCE(cm.container_number, '')
        )
        FROM (
          SELECT (cd_obj->>'cid')::int AS cid
          FROM jsonb_array_elements(COALESCE(oi.container_details, '[]'::jsonb)) AS cd_obj
          WHERE (cd_obj->>'cid') ~ '^\\d+$'
          UNION
          SELECT cid FROM container_assignment_history WHERE detail_id = oi.id GROUP BY cid
        ) u (cid)
        LEFT JOIN container_master cm ON u.cid = cm.cid
        LEFT JOIN LATERAL (SELECT availability AS derived_status FROM container_status WHERE cid = u.cid ORDER BY sid DESC LIMIT 1) cs ON true
        LEFT JOIN LATERAL (SELECT COALESCE(SUM(assigned_qty), 0) AS assigned_qty FROM container_assignment_history WHERE detail_id = oi.id AND cid = u.cid) ass ON true
        LEFT JOIN LATERAL (SELECT COALESCE(SUM(assigned_qty), 0) AS total_ass FROM container_assignment_history WHERE detail_id = oi.id) tot ON true
        WHERE u.cid IS NOT NULL
      ), '[]'::json)
    `;

    // NEW: Aggregate drop_off_details per receiver (grouped by receiver_id)
    const dropOffDetailsSubquery = `
      COALESCE((
        SELECT json_object_agg(
          r2.id,
          dod.receiver_dropoffs
        )
        FROM receivers r2
        LEFT JOIN LATERAL (
          SELECT json_agg(
            json_build_object(
              'dropMethod', dod.drop_method,
              'dropoffName', dod.dropoff_name,
              'dropOffCnic', dod.drop_off_cnic,
              'dropOffMobile', dod.drop_off_mobile,
              'plateNo', dod.plate_no,
              'dropDate', dod.drop_date
            ) ORDER BY dod.id
          ) AS receiver_dropoffs
          FROM drop_off_details dod
          WHERE dod.receiver_id = r2.id
        ) dod ON true
        WHERE r2.order_id = o.id
      ), '{}'::json)
    `;

    let selectFields = [
      'o.*',
      's.sender_name, s.sender_contact, s.sender_address, s.sender_email, s.sender_ref, s.sender_type, s.selected_sender_owner',
      't.transport_type, t.third_party_transport, t.driver_name, t.driver_contact, t.driver_nic',
      't.driver_pickup_location, t.truck_number, t.drop_method, t.dropoff_name, t.drop_off_cnic',
      't.drop_off_mobile, t.plate_no, t.drop_date, t.collection_method, t.collection_scope, t.qty_delivered',
      't.client_receiver_name, t.client_receiver_id, t.client_receiver_mobile, t.delivery_date, t.gatepass',
      'ot.status AS tracking_status, ot.created_time AS tracking_created_time, ot.container_id',
      'cm.container_number',
      // Receiver containers (string agg)
      `COALESCE((SELECT string_agg(DISTINCT elem, ', ') 
                 FROM (SELECT jsonb_array_elements_text(r3.containers) AS elem 
                       FROM receivers r3 
                       WHERE r3.order_id = o.id 
                         AND r3.containers IS NOT NULL 
                         AND jsonb_typeof(r3.containers) = 'array') AS unnested), '') AS receiver_containers_json`,
      // Full receivers with nested shippingDetails and dropOffDetails
      `(SELECT COALESCE(json_agg(r2_full ORDER BY r2_full.position), '[]') FROM (
        SELECT 
          r2.*,
          (ROW_NUMBER() OVER (ORDER BY r2.id) - 1) AS position,  -- For frontend receiver_index
          sd_full.shippingDetails,
          dod_full.dropOffDetails
        FROM receivers r2
        LEFT JOIN LATERAL (
          SELECT json_agg(
            json_build_object(
              'id', oi.id,
              'category', COALESCE(oi.category, ''),
              'subcategory', COALESCE(oi.subcategory, ''),
              'type', COALESCE(oi.type, ''),
              'pickupLocation', COALESCE(oi.pickup_location, ''),
              'deliveryAddress', COALESCE(oi.delivery_address, ''),
              'totalNumber', ${safeIntCast('oi.total_number')},
              'weight', ${safeIntCast('oi.weight')},
              'itemRef', COALESCE(oi.item_ref, ''),
              'containerDetails', ${containerDetailsSub},
              'remainingItems', ${safeIntCast('oi.total_number')} - ${totalAssignedSub}
            ) ORDER BY oi.id
          ) AS shippingDetails
          FROM order_items oi WHERE oi.receiver_id = r2.id
        ) sd_full ON true
        LEFT JOIN LATERAL (
          SELECT json_agg(
            json_build_object(
              'dropMethod', dod.drop_method,
              'dropoffName', dod.dropoff_name,
              'dropOffCnic', dod.drop_off_cnic,
              'dropOffMobile', dod.drop_off_mobile,
              'plateNo', dod.plate_no,
              'dropDate', dod.drop_date
            ) ORDER BY dod.id
          ) AS dropOffDetails
          FROM drop_off_details dod WHERE dod.receiver_id = r2.id
        ) dod_full ON true
        WHERE r2.order_id = o.id
      ) r2_full) AS receivers`,
      // Optional: Separate top-level dropOffDetails if you want it flattened or indexed
      `${dropOffDetailsSubquery} AS drop_off_details_agg`
    ].join(', ');

    const joinsArray = [
      'LEFT JOIN senders s ON o.id = s.order_id',
      'LEFT JOIN transport_details t ON o.id = t.order_id',
      'LEFT JOIN LATERAL (SELECT ot2.status, ot2.created_time, ot2.container_id FROM order_tracking ot2 WHERE ot2.order_id = o.id ORDER BY ot2.created_time DESC LIMIT 1) ot ON true',
      'LEFT JOIN container_master cm ON ot.container_id = cm.cid'
    ];
    const joins = joinsArray.join('\n ');

    const countQuery = `
      SELECT COUNT(DISTINCT o.id) as total_count
      FROM orders o
      ${joins}
      ${whereClause}
    `;

    const mainQuery = `
      SELECT ${selectFields}
      FROM orders o
      ${joins}
      ${whereClause}
      ORDER BY o.created_at DESC
      LIMIT $${params.length + 1} OFFSET $${params.length + 2}
    `;

    params.push(safeLimit, safeOffset);

    const [result, countResult] = await Promise.all([
      pool.query(mainQuery, params),
      pool.query(countQuery, params.slice(0, -2))
    ]);

    const total = parseInt(countResult.rows[0]?.total_count || 0);

    res.json({
      data: result.rows,
      pagination: {
        page: safePage,
        limit: safeLimit,
        total,
        totalPages: total === 0 ? 0 : Math.ceil(total / safeLimit)
      }
    });

  } catch (err) {
    console.error("Error fetching orders:", err);
    res.status(500).json({
      error: 'Failed to fetch orders',
      details: err.code === '42703' ? 'Column or table not found. Check drop_off_details table exists.' : err.message
    });       
  }
}   
// const { pool } = require('../config/database');  // Adjust path
// const { getOrderStatusColor } = require('../utils/statusUtils');  // Assume exists

export async function getMyOrdersByRef(req, res) {
  try {
    const userId = req.user.sub;
    if (!userId) {
      return res.status(401).json({ error: 'User ID not found in token' });
    }

    const { limit = 20, offset = 0, status, search } = req.query;
    const safeLimit = Math.min(50, Math.max(1, parseInt(limit) || 20));
    const safeOffset = parseInt(offset) || 0;

    // ────────────────────────────────────────────────
    // Base WHERE clause (user + optional filters)
    // ────────────────────────────────────────────────
    let whereClauses = ['o.user_id = $1'];
    let params = [userId];

    if (status) {
      whereClauses.push(`o.status = $${params.length + 1}`);
      params.push(status);
    }

    if (search && search.trim()) {
      const term = `%${search.trim()}%`;
      whereClauses.push(`(
        o.booking_ref ILIKE $${params.length + 1} OR
        o.rgl_booking_number ILIKE $${params.length + 1} OR
        EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.item_ref ILIKE $${params.length + 1})
      )`);
      params.push(term);
    }

    const whereSql = whereClauses.length > 0 ? 'WHERE ' + whereClauses.join(' AND ') : '';

    // ────────────────────────────────────────────────
    // Main query – rich order data
    // ────────────────────────────────────────────────
    const ordersQuery = `
      SELECT 
        o.*,
        s.sender_name, s.sender_contact, s.sender_email, s.sender_address,
        t.transport_type, t.driver_name, t.driver_contact, t.truck_number, t.drop_method, t.delivery_date,
        ot.status AS latest_tracking_status,
        ot.created_time AS latest_tracking_time,
        -- Receiver summary
        (SELECT json_agg(
           json_build_object(
             'id', r.id,
             'receiver_name', r.receiver_name,
             'receiver_contact', r.receiver_contact,
             'receiver_email', r.receiver_email,
             'receiver_address', r.receiver_address,
             'status', r.status,
             'eta', r.eta,
             'containers', r.containers
           )
           ORDER BY r.id
         ) FROM receivers r WHERE r.order_id = o.id) AS receivers_summary,
        -- Item summary with container assignments
        (SELECT json_agg(
           json_build_object(
             'item_id', oi.id,
             'item_ref', oi.item_ref,
             'category', oi.category,
             'subcategory', oi.subcategory,
             'type', oi.type,
             'total_number', oi.total_number,
             'weight', oi.weight,
             'assigned_qty', COALESCE((
               SELECT SUM(cah.assigned_qty)
               FROM container_assignment_history cah
               WHERE cah.detail_id = oi.id
             ), 0),
             'remaining', oi.total_number - COALESCE((
               SELECT SUM(cah.assigned_qty)
               FROM container_assignment_history cah
               WHERE cah.detail_id = oi.id
             ), 0)
           )
           ORDER BY oi.id
         ) FROM order_items oi WHERE oi.order_id = o.id) AS items_summary
      FROM orders o
      LEFT JOIN senders s ON s.order_id = o.id
      LEFT JOIN transport_details t ON t.order_id = o.id
      LEFT JOIN LATERAL (
        SELECT status, created_time
        FROM order_tracking
        WHERE order_id = o.id
        ORDER BY created_time DESC LIMIT 1
      ) ot ON true
      ${whereSql}
      ORDER BY o.created_at DESC
      LIMIT $${params.length + 1} OFFSET $${params.length + 2}
    `;

    params.push(safeLimit, safeOffset);

    const ordersResult = await pool.query(ordersQuery, params);

    if (ordersResult.rowCount === 0) {
      return res.json({
        success: true,
        data: [],
        message: 'No orders found',
        count: 0
      });
    }

    // Optional: enrich with full assignment history (only if needed – can be heavy)
    // If you want full history per order, do a second query or lateral join

    const enriched = ordersResult.rows.map(row => ({
      ...row,
      overall_status: row.latest_tracking_status || row.status || 'Created',
      // you can add computed fields here, e.g. progress = assigned / total
    }));

    // Optional: total count for pagination
    const countQuery = `SELECT COUNT(*) FROM orders o ${whereSql}`;
    const countRes = await pool.query(countQuery, params.slice(0, -2));
    const total = parseInt(countRes.rows[0].count);

    res.json({
      success: true,
      data: enriched,
      pagination: {
        total,
        limit: safeLimit,
        offset: safeOffset,
        pages: Math.ceil(total / safeLimit)
      }
    });

  } catch (err) {
    console.error('getMyOrdersByRef error:', err);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch your orders',
      error: err.message
    });
  }
}
export async function getOrdersConsignments(req, res) {
  try {
    // FIXED: Sanitize page and limit early to avoid NaN in offset calc
    const rawPage = req.query.page || '1';
    const rawLimit = req.query.limit || '10';
    const safePage = Math.max(1, parseInt(rawPage) || 1);
    const safeLimit = Math.max(1, Math.min(100, parseInt(rawLimit) || 10)); // Clamp 1-100
    const safeOffset = (parseInt(safePage) - 1) * safeLimit;
    const { status, booking_ref, container_id } = req.query;
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
    let containerNumbers = []; // To store looked-up container numbers
    if (container_id) {
      const containerIds = container_id.split(',').map(id => id.trim()).filter(Boolean);
      if (containerIds.length > 0) {
        // FIXED: Filter valid numeric IDs to avoid NaN/empty in array
        const validIds = containerIds.filter(id => !isNaN(parseInt(id)));
        if (validIds.length === 0) {
          // No valid containers, early return empty
          return res.json({
            data: [],
            pagination: {
              page: safePage,
              limit: safeLimit,
              total: 0,
              totalPages: 0
            }
          });
        }
        const idArray = validIds.map(id => parseInt(id)); // Now safe: all ints
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
              page: safePage,
              limit: safeLimit,
              total: 0,
              totalPages: 0
            }
          });
        }
        // Conditions for ot.container_id (exact numeric match on CIDs)
        const otConditions = validIds.map((idStr) => {
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
        // FIXED: Conditions for receivers JSONB (add jsonb_typeof check for safety)
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
            AND jsonb_typeof(r.containers) = 'array'
            AND (${receiverExists})
          )
        )`;
      }
    }
    const safeIntCast = (val) => `COALESCE(${val}, 0)`;
    const safeNumericCast = (val) => `COALESCE(${val}, 0)`;
    const totalAssignedSub = `(SELECT COALESCE(SUM(assigned_qty), 0) FROM container_assignment_history WHERE detail_id = oi.id)`;
    const containerDetailsSub = `
      COALESCE((
        SELECT json_agg(
          json_build_object(
            'status', COALESCE(cs.derived_status, CASE WHEN ass.assigned_qty > 0 THEN 'Ready for Loading' ELSE 'Created' END),
            'container', json_build_object(
              'cid', u.cid,
              'container_number', COALESCE(cm.container_number, '')
            ),
            'total_number', ${safeIntCast('oi.total_number')},
            'assign_weight', CASE 
              WHEN tot.total_ass > 0 AND ass.assigned_qty > 0 THEN 
                ROUND((ass.assigned_qty::numeric / tot.total_ass::numeric * ${safeNumericCast('oi.weight')} / 1000), 2)::text 
              ELSE '0' 
            END,
            'remaining_items', (${safeIntCast('oi.total_number')} - COALESCE(tot.total_ass, 0))::text,
            'assign_total_box', COALESCE(ass.assigned_qty, 0)::text
          ) ORDER BY COALESCE(cm.container_number, '')
        )
        FROM (
          SELECT (cd_obj->>'cid')::int AS cid
          FROM jsonb_array_elements(COALESCE(oi.container_details, '[]'::jsonb)) AS cd_obj
          WHERE (cd_obj->>'cid') ~ '^\\d+$'
          UNION
          SELECT cid
          FROM container_assignment_history
          WHERE detail_id = oi.id
          GROUP BY cid
        ) u (cid)
        LEFT JOIN container_master cm ON u.cid = cm.cid
        LEFT JOIN LATERAL (
          SELECT availability AS derived_status
          FROM container_status
          WHERE cid = u.cid
          ORDER BY sid DESC NULLS LAST
          LIMIT 1
        ) cs ON true
        LEFT JOIN LATERAL (
          SELECT COALESCE(SUM(assigned_qty), 0) AS assigned_qty
          FROM container_assignment_history
          WHERE detail_id = oi.id AND cid = u.cid
        ) ass ON true
        LEFT JOIN LATERAL (
          SELECT COALESCE(SUM(assigned_qty), 0) AS total_ass
          FROM container_assignment_history
          WHERE detail_id = oi.id
        ) tot ON true
        WHERE u.cid IS NOT NULL
      ), '[]'::json)
    `;
let selectFields = [
  'o.*', // Core orders
  's.sender_name, s.sender_contact, s.sender_address, s.sender_email, s.sender_ref, s.sender_type, s.selected_sender_owner', // From senders
  't.transport_type, t.third_party_transport, t.driver_name, t.driver_contact, t.driver_nic', // From transport_details
  't.driver_pickup_location, t.truck_number, t.drop_method, t.dropoff_name, t.drop_off_cnic',
  't.drop_off_mobile, t.plate_no, t.drop_date, t.collection_method, t.collection_scope, t.qty_delivered', // From transport_details
  't.client_receiver_name, t.client_receiver_id, t.client_receiver_mobile, t.delivery_date',
  't.gatepass', // From transport_details
  'ot.status AS tracking_status, ot.created_time AS tracking_created_time', // Latest tracking
  'ot.container_id', // Explicit for join
  'cm.container_number', // From container_master
  // FIXED: Subquery for aggregated containers from all receivers (add jsonb_typeof for safety)
  'COALESCE((SELECT string_agg(DISTINCT elem, \', \') FROM (SELECT jsonb_array_elements_text(r3.containers) AS elem FROM receivers r3 WHERE r3.order_id = o.id AND r3.containers IS NOT NULL AND jsonb_typeof(r3.containers) = \'array\' AND jsonb_array_length(r3.containers) > 0) AS unnested), \'\') AS receiver_containers_json',
  // Updated subquery for full receivers as JSON array per order, using LATERAL with fixed ORDER BY position
  // FIXED: Ensure shippingDetails is always an array (COALESCE to '[]' if null)
  `(SELECT COALESCE(json_agg(r2_full ORDER BY COALESCE(r2_full.id, 0)), '[]') FROM (
    SELECT 
      r2.id, r2.order_id, r2.receiver_name, r2.receiver_contact, r2.receiver_address, r2.receiver_email,
      r2.total_number, r2.total_weight, r2.receiver_ref, r2.remarks, r2.containers,
      r2.status, r2.eta, r2.etd, r2.shipping_line, r2.consignment_vessel, r2.consignment_number,
      r2.consignment_marks, r2.consignment_voyage, r2.full_partial, r2.qty_delivered,
      COALESCE(sd_full.shippingDetails, '[]'::json) AS shippingDetails
    FROM receivers r2
    LEFT JOIN LATERAL (
      SELECT json_agg(
        json_build_object(
          'id', oi.id,
          'order_id', oi.order_id,
          'sender_id', oi.sender_id,
          'category', COALESCE(oi.category, ''),
          'subcategory', COALESCE(oi.subcategory, ''),
          'type', COALESCE(oi.type, ''),
          'pickupLocation', COALESCE(oi.pickup_location, ''),
          'deliveryAddress', COALESCE(oi.delivery_address, ''),
          'totalNumber', ${safeIntCast('oi.total_number')},
          'weight', ${safeNumericCast('oi.weight')},
          'totalWeight', ${safeNumericCast('oi.total_weight')},
          'itemRef', COALESCE(oi.item_ref, ''),
          'consignmentStatus', COALESCE(oi.consignment_status, ''),
          'shippingLine', COALESCE(oi.shipping_line, ''),
          'containerDetails', ${containerDetailsSub},
          'remainingItems', ${safeIntCast('oi.total_number')} - ${totalAssignedSub}
        ) ORDER BY oi.id
      ) AS shippingDetails
      FROM order_items oi
      WHERE oi.receiver_id = r2.id
    ) sd_full ON true
    WHERE r2.order_id = o.id
      AND r2.id IS NOT NULL  -- FIXED: Exclude any null/ghost receivers
  ) r2_full) AS receivers`
].join(', ');
    // Build joins as array for easier extension (removed receivers join, now in subquery)
    let joinsArray = [
      'LEFT JOIN senders s ON o.id = s.order_id',
      'LEFT JOIN transport_details t ON o.id = t.order_id',
      'LEFT JOIN LATERAL (SELECT ot2.status, ot2.created_time, ot2.container_id FROM order_tracking ot2 WHERE ot2.order_id = o.id ORDER BY ot2.created_time DESC LIMIT 1) ot ON true', // Latest tracking
      'LEFT JOIN container_master cm ON ot.container_id = cm.cid' // Join to container_master on cid
    ];
    const joins = joinsArray.join('\n ');
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
    // const safeOffset = (parseInt(safePage) - 1) * safeLimit;
    // FIXED: Ensure limit/offset are always valid ints (fallback if NaN)
    params.push(safeLimit, safeOffset);
    const [result, countResult] = await Promise.all([
      pool.query(query, params),
      pool.query(countQuery, params.slice(0, -2)) // without limit offset
    ]);
    const total = parseInt(countResult.rows[0].total_count || 0);
    res.json({
      data: result.rows,
      pagination: {
        page: safePage,
        limit: safeLimit,
        total,
        totalPages: total === 0 ? 0 : Math.ceil(total / safeLimit)
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

export async function getOrderById(req, res) {
  let client; // For potential tx if needed for calculateETA
  try {
    const { id } = req.params;
    const { includeContainer = 'true' } = req.query;

    client = await pool.connect(); // Connect for calculateETA if needed

    // FIXED: Simplified SELECT fields - removed tracking and container joins to avoid schema mismatch
    // Only core tables: orders, senders, transport_details
    let selectFields = [
      'o.*',  // Core orders
      's.sender_name, s.sender_contact, s.sender_address, s.sender_email, s.sender_ref, s.sender_remarks, s.sender_type, s.selected_sender_owner',  // From senders
      't.transport_type, t.third_party_transport, t.driver_name, t.driver_contact, t.driver_nic',  // From transport_details
      't.driver_pickup_location, t.truck_number, t.drop_method, t.dropoff_name, t.drop_off_cnic',
      't.drop_off_mobile, t.plate_no, t.drop_date, t.collection_method, t.collection_scope, t.qty_delivered',  // From transport_details
      't.client_receiver_name, t.client_receiver_id, t.client_receiver_mobile, t.delivery_date',
      't.gatepass'  // From transport_details
    ].join(', ');

    // FIXED: Simplified joins - only senders and transport_details
    let joinsArray = [
      'LEFT JOIN senders s ON o.id = s.order_id',
      'LEFT JOIN transport_details t ON o.id = t.order_id'
    ];

    const joins = joinsArray.join('\n      ');

    const query = `
      SELECT ${selectFields}
      FROM orders o
      ${joins}
      WHERE o.id = $1
    `;

    const orderResult = await client.query(query, [id]);
    if (orderResult.rowCount === 0) {
      return res.status(404).json({ error: 'Order not found' });
    }

    const orderRow = orderResult.rows[0];

    // Log total_assigned_qty for debugging (aligned with createOrder's aggregation potential)
    console.log(`[getOrderById ${id}] Order fetched with total_assigned_qty: ${orderRow.total_assigned_qty || 0}`);

    // Updated: Receivers query aligned with createOrder (eta/etd per receiver, containers init empty)
    // Fetch receivers with nested shippingDetails (including container_details from order_items) and remaining_items from containerDetails JSON sum, with fixed ORDER BY position
    // FIXED: For DB numeric columns, use simple COALESCE(val, 0) to handle NULLs without type coercion errors
    // For JSON-extracted text fields, use full safe casting with trimming to handle potential invalid strings/spaces
    const safeIntCast = (val) => `COALESCE(${val}, 0)`;
    const safeNumericCast = (val) => `COALESCE(${val}, 0)`;
    const safeIntFromJson = (jsonPath) => {
      const trimmed = `TRIM(COALESCE(${jsonPath}, ''))`;
      return `CASE 
        WHEN ${trimmed} = '' THEN 0
        WHEN ${trimmed} ~ '^\\d+$' THEN ${trimmed}::int
        ELSE 0 
      END`;
    };
    const totalAssignedSub = `(SELECT COALESCE(SUM(assigned_qty), 0) FROM container_assignment_history WHERE detail_id = oi.id)`;
    const containerDetailsSub = `
      COALESCE((
        SELECT json_agg(
          json_build_object(
            'status', COALESCE(cs.derived_status, CASE WHEN ass.assigned_qty > 0 THEN 'Ready for Loading' ELSE 'Created' END),
            'container', json_build_object(
              'cid', u.cid,
              'container_number', COALESCE(cm.container_number, '')
            ),
            'total_number', ${safeIntCast('oi.total_number')},
            'assign_weight', CASE 
              WHEN tot.total_ass > 0 AND ass.assigned_qty > 0 THEN 
                ROUND((ass.assigned_qty::numeric / tot.total_ass::numeric * ${safeNumericCast('oi.weight')} / 1000), 2)::text 
              ELSE '0' 
            END,
            'remaining_items', (${safeIntCast('oi.total_number')} - COALESCE(tot.total_ass, 0))::text,
            'assign_total_box', COALESCE(ass.assigned_qty, 0)::text
          ) ORDER BY COALESCE(cm.container_number, '')
        )
        FROM (
          SELECT (cd_obj->>'cid')::int AS cid
          FROM jsonb_array_elements(COALESCE(oi.container_details, '[]'::jsonb)) AS cd_obj
          WHERE (cd_obj->>'cid') ~ '^\\d+$'
          UNION
          SELECT cid
          FROM container_assignment_history
          WHERE detail_id = oi.id
          GROUP BY cid
        ) u (cid)
        LEFT JOIN container_master cm ON u.cid = cm.cid
        LEFT JOIN LATERAL (
          SELECT availability AS derived_status
          FROM container_status
          WHERE cid = u.cid
          ORDER BY sid DESC NULLS LAST
          LIMIT 1
        ) cs ON true
        LEFT JOIN LATERAL (
          SELECT COALESCE(SUM(assigned_qty), 0) AS assigned_qty
          FROM container_assignment_history
          WHERE detail_id = oi.id AND cid = u.cid
        ) ass ON true
        LEFT JOIN LATERAL (
          SELECT COALESCE(SUM(assigned_qty), 0) AS total_ass
          FROM container_assignment_history
          WHERE detail_id = oi.id
        ) tot ON true
        WHERE u.cid IS NOT NULL
      ), '[]'::json)
    `;

    const receiversQuery = `
      SELECT 
        r.id, r.order_id, r.receiver_name, r.receiver_contact, r.receiver_address, r.receiver_email,
        ${safeIntCast('r.total_number')} AS total_number,
        ${safeNumericCast('r.total_weight')} AS total_weight,
        r.receiver_ref, r.remarks, r.containers,
        r.status, r.eta, r.etd, r.shipping_line, r.consignment_vessel, r.consignment_number,
        r.consignment_marks, r.consignment_voyage, r.full_partial,
        ${safeIntCast('r.qty_delivered')} AS qty_delivered,
        sd_full.shippingdetails
      FROM receivers r
      LEFT JOIN LATERAL (
        SELECT json_agg(
          json_build_object(
            'id', oi.id,
            'order_id', oi.order_id,
            'sender_id', oi.sender_id,
            'category', COALESCE(oi.category, ''),
            'subcategory', COALESCE(oi.subcategory, ''),
            'type', COALESCE(oi.type, ''),
            'pickupLocation', COALESCE(oi.pickup_location, ''),
            'deliveryAddress', COALESCE(oi.delivery_address, ''),
            'totalNumber', ${safeIntCast('oi.total_number')},
            'weight', ${safeNumericCast('oi.weight')},
            'totalWeight', ${safeNumericCast('oi.total_weight')},
            'itemRef', COALESCE(oi.item_ref, ''),
            'consignmentStatus', COALESCE(oi.consignment_status, ''),
            'shippingLine', COALESCE(oi.shipping_line, ''),
            'containerDetails', ${containerDetailsSub},
            'remainingItems', (${safeIntCast('oi.total_number')} - COALESCE((SELECT SUM(${safeIntFromJson('elem->>\'assign_total_box\'')}) FROM jsonb_array_elements(COALESCE(oi.container_details, '[]'::jsonb)) AS elem), 0))
          ) ORDER BY oi.id
        ) AS shippingdetails
        FROM order_items oi
        WHERE oi.receiver_id = r.id
      ) sd_full ON true
      WHERE r.order_id = $1
      ORDER BY r.id
    `;
    const receiversResult = await client.query(receiversQuery, [id]);
    let receivers = receiversResult.rows.map(row => ({
      ...row,
      shippingDetails: row.shippingdetails || [],  // Ensure array even if no items; rename to camelCase
      containers: typeof row.containers === 'string' ? JSON.parse(row.containers) : (row.containers || [])  // Clean malformed JSON
    }));

    // NEW: Fetch all containers to enrich containerDetails with container_number
    const allContainersQuery = `
      SELECT cid, container_number 
      FROM container_master
      ORDER BY cid
    `;
    const allContainersResult = await client.query(allContainersQuery);
    const allContainersMap = new Map(allContainersResult.rows.map(c => [c.cid, c.container_number]));

    // Enrich each receiver's shippingDetails.containerDetails with container_number
    receivers.forEach(receiver => {
      receiver.shippingDetails.forEach(sd => {
        if (sd.containerDetails && Array.isArray(sd.containerDetails)) {
          sd.containerDetails.forEach(cd => {
            const containerCid = cd.container;
            if (typeof containerCid === 'number') {
              const containerNumber = allContainersMap.get(containerCid) || '';
              cd.container = {
                cid: containerCid,
                container_number: containerNumber
              };
            }
            // If already object, assume it's enriched; skip
          });
        }
      });
    });

    // ==================== NEW: FETCH DROP_OFF_DETAILS ====================
    const dropOffQuery = `
      SELECT 
        receiver_id,
        json_agg(
          json_build_object(
            'drop_method', drop_method,
            'dropoff_name', dropoff_name,
            'drop_off_cnic', drop_off_cnic,
            'drop_off_mobile', drop_off_mobile,
            'plate_no', plate_no,
            'drop_date', TO_CHAR(drop_date, 'YYYY-MM-DD')
          ) ORDER BY id
        ) AS drop_off_details
      FROM drop_off_details
      WHERE order_id = $1
      GROUP BY receiver_id
    `;

    const dropOffResult = await client.query(dropOffQuery, [id]);

    // Create a map: receiver_id → array of drop_off_details
    const dropOffMap = new Map();
    dropOffResult.rows.forEach(row => {
      dropOffMap.set(row.receiver_id, row.drop_off_details || []);
    });

    // Attach drop_off_details to each receiver
    receivers = receivers.map(receiver => ({
      ...receiver,
      drop_off_details: dropOffMap.get(receiver.id) || []  // Empty array if none
    }));
    // =====================================================================

    // Fetch container_assignment_history details for this order
    const historyQuery = `
      SELECT 
        h.*,
        cm.container_number
      FROM container_assignment_history h
      LEFT JOIN container_master cm ON h.cid = cm.cid
      WHERE h.order_id = $1
      ORDER BY h.id DESC  -- FIXED: Use 'id' instead of 'created_time' assuming auto-increment primary key
    `;
    const historyResult = await client.query(historyQuery, [id]);

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
    const receiversWithContainers = receivers.filter(r => {
      return (r.shippingDetails || []).some(sd => 
        (sd.containerDetails || []).some(cd => cd.container && cd.container.cid)
      );
    });
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
      receivers,  // Now includes drop_off_details per receiver
      assignmentHistory: historyResult.rows,  // NEW: Added full history details
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
    if (err.code === '22P02') {  // NEW: Handle invalid input syntax (casting errors)
      return res.status(500).json({ error: 'Invalid data format in order details. Please check numeric fields.' });
    } 
    res.status(500).json({ error: 'Failed to fetch order', details: err.message });
  } finally {
    if (client) {
      client.release();
    }
  }
}

function computeDaysUntilEta(etaDateStr, today = new Date()) {  // Dynamic: Default to current date
  if (!etaDateStr) return null;
  const etaDate = new Date(etaDateStr);
  if (isNaN(etaDate.getTime())) return null;  // Invalid date guard
  const diffTime = etaDate - today;
  const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
  return diffDays;  // Allow negative for past ETAs
}
const CONSIGNMENT_TO_STATUS_MAP = {
  // Consignment Status          → Container Status          → Shipment (Receiver) Status      → ETA from DB
  'Customs Cleared':            { container: 'Shipment Processing',   shipment: 'Shipment Processing' },           // 7 days
  'Submitted On Vessel':        { container: 'Shipment Processing',   shipment: 'Shipment Processing' },           // 7 days
  'Submitted':                  { container: 'Shipment Processing',   shipment: 'Shipment Processing' },           // 7 days (if needed)
  'In Transit':                 { container: 'In Transit',            shipment: 'Shipment In Transit' },           // 4 days
  'Ready for Delivery':         { container: 'Ready for Delivery',    shipment: 'Ready for Delivery' },            // 0 days
  'Arrived at Destination':     { container: 'Under Processing',     shipment: 'Under Processing' },              // 2 days
  'Loaded':                     { container: 'Loaded',                shipment: 'Loaded Into Container' },         // 9 days
  'Ready for loading':          { container: 'Ready for Loading',     shipment: 'Ready for Loading' },             // 12 days
  'Created':                    { container: 'Created',               shipment: 'Order Created' },                 // 15 days (or 'Created' → 15)
  'Arrived':                    { container: 'Arrived at Sort Facility', shipment: 'Arrived at Sort Facility' },   // 1 day
  'De-Linked':                  { container: 'Arrived at Sort Facility', shipment: 'Arrived at Sort Facility' },   // 1 day
  'Delivered':                  { container: 'Delivered',             shipment: 'Shipment Delivered' },            // 0 days
};
// Helper: Wrap in transaction
async function withTransaction(operation) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await operation(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

// Helper: Send notification (placeholder—integrate with your GAS/notifications module)
async function sendNotification(consignmentData, event = 'created') {
  // e.g., await emailService.send({ to: consignmentData.consignee.email, subject: `Consignment ${consignmentData.consignment_number} ${event}` });
  console.log(`Notification sent for consignment ${consignmentData.consignment_number}: ${event}`);
}

// Unified logging function: Handles both 'logToTracking' and 'safeLogToTracking' calls
async function logToTracking(client, consignmentId, eventType = 'unknown', logData = {}) {
  // Validate eventType (required, non-null)
  if (!eventType || typeof eventType !== 'string' || eventType.trim() === '') {
    console.error(`Invalid eventType '${eventType}' for consignment ${consignmentId} – defaulting to 'unknown_event'`);
    eventType = 'unknown_event';  // Fallback to avoid NULL violation
  }

  // Validate against schema CHECK (expand as needed)
  const validEvents = ['status_advanced', 'status_updated', 'status_auto_updated', 'updated', 'order_synced'];
  if (!validEvents.includes(eventType)) {
    console.warn(`Event '${eventType}' not in DB CHECK – add to constraint or use valid one`);
  }

  try {
    // Normalize logData
    const {
      from: oldStatus = null,
      to: newStatus = null,
      offsetDays = 0,
      reason = null,
      ...extraDetails
    } = logData;

    const details = {
      ...extraDetails,
      old_status: oldStatus,
      new_status: newStatus,
      reason,
      action: logData.action || eventType  // Legacy: Store 'action' in details if passed
    };

    const query = `
      INSERT INTO consignment_tracking (
        consignment_id, event_type, old_status, new_status, offset_days, details
      ) VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (consignment_id, event_type, timestamp) DO NOTHING
      RETURNING id
    `;
    const result = await client.query(query, [
      consignmentId,
      eventType.trim(),  // Ensure non-null string
      oldStatus,
      newStatus,
      offsetDays,
      details
    ]);

    if (result.rowCount > 0) {
      console.log(`✓ Logged '${eventType}' for ${consignmentId} (ID: ${result.rows[0].id})`);
      return { success: true, id: result.rows[0].id };
    } else {
      console.log(`⚠ Duplicate '${eventType}' skipped for ${consignmentId}`);
      return { success: true, skipped: true };
    }
  } catch (error) {
    console.error(`Failed to log '${eventType}' for ${consignmentId}:`, error);
    if (error.code === '23502') {
      console.error('NOT NULL violation on event_type – ensure non-null param');
    } else if (error.code === '23514') {
      console.error(`CHECK violation: '${eventType}' not allowed – update DB constraint`);
    }
    return { success: false, error: error.message };
    // No throw – keep tx alive
  }
}
async function safeLogToTracking(client, consignmentId, eventType, logData = {}) {
  // Validate event_type against schema CHECK (optional, but prevents 23514 errors)
  const validEvents = ['status_advanced', 'status_updated', 'order_synced', 'status_auto_updated'];  // Sync with DB
  if (!validEvents.includes(eventType)) {
    console.warn(`Invalid event_type '${eventType}' – add to DB CHECK constraint`);
    return { success: false, reason: 'Invalid event' };
  }
  try {
    // Normalize: Use eventType as event_type; ignore/rename 'action' if present
    const {
      from: oldStatus = null,
      to: newStatus = null,
      offsetDays = 0,
      reason = null,
      action,  // Ignore if passed; use eventType
      ...extraDetails
    } = logData;

    const details = {
      ...extraDetails,
      old_status: oldStatus,
      new_status: newStatus,
      reason,
      action: action || eventType  // Legacy: Store in details if needed
    };

    const query = `
      INSERT INTO consignment_tracking (
        consignment_id, event_type, old_status, new_status, offset_days, details
      ) VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (consignment_id, event_type, timestamp) DO NOTHING
      RETURNING id
    `;
    const result = await client.query(query, [
      consignmentId,
      eventType,  // Use this for event_type (e.g., 'status_auto_updated')
      oldStatus,
      newStatus,
      offsetDays,
      details
    ]);

    if (result.rowCount > 0) {
      console.log(`✓ Logged '${eventType}' for ${consignmentId} (ID: ${result.rows[0].id})`);
      return { success: true, id: result.rows[0].id };
    } else {
      console.log(`⚠ Duplicate '${eventType}' skipped for ${consignmentId}`);
      return { success: true, skipped: true };
    }
  } catch (error) {
    console.error(`Failed to log '${eventType}' for ${consignmentId}:`, error);
    if (error.code === '42703') {
      console.error('Schema mismatch – check INSERT columns vs. table (e.g., no "action" column)');
    }
    return { success: false, error: error.message };
    // No throw – non-critical
  }
}
// Helper to extract valid integer IDs from orderIds (handles array of IDs or array of objects; robust parsing)
function extractOrderIds(orderData) {
  if (!orderData || !Array.isArray(orderData)) {
    return [];
  }

  const ids = orderData
    .map(item => {
      let id = null;
      if (typeof item === 'number') {
        id = item;
      } else if (typeof item === 'object' && item !== null) {
        if ('id' in item && item.id !== null && item.id !== undefined) {
          id = parseInt(item.id, 10);
        } else if ('value' in item || 'key' in item) {
          const val = item.value || item.key;
          id = parseInt(val, 10);
        }
      } else if (typeof item === 'string') {
        id = parseInt(item, 10);
      }
      return id;
    })
    .filter(id => Number.isInteger(id) && id > 0);  // Strict: integer and positive

  return ids;
}

export async function advanceStatus(req, res) {
  console.log("Advance Status Request Params:", req.params);    

  let syncOrderIds = []; // Declare outside transaction

  try {
    const { id } = req.params;
    const numericId = parseInt(id, 10);
    if (isNaN(numericId) || numericId <= 0) {
      return res.status(400).json({ error: 'Invalid consignment ID.' });
    }

    const { rows } = await pool.query('SELECT status FROM consignments WHERE id = $1', [numericId]);
    if (rows.length === 0) {
      return res.status(404).json({ error: 'Consignment not found' });
    }

    const currentStatus = rows[0].status;

    // Next status for consignment
    const nextStatusMap = {
      'Drafts Cleared': 'Submitted On Vessel',
      'Submitted On Vessel': 'Customs Cleared',
      'Customs Cleared': 'Submitted',
      'Submitted': 'Under Shipment Processing',
      'Under Shipment Processing': 'In Transit',
      'In Transit': 'Arrived at Facility',
      'Arrived at Facility': 'Ready for Delivery',
      'Ready for Delivery': 'Arrived at Destination',
      'Arrived at Destination': 'Delivered',
    };

    const nextStatus = nextStatusMap[currentStatus];
    if (!nextStatus) {
      return res.status(400).json({ error: `No next status defined from "${currentStatus}"` });
    }

    // Sync mapping (for orders/receivers/containers)
    const statusSyncMap = {
      'Drafts Cleared': 'Order Created',
      'Submitted On Vessel': 'Ready for Loading',
      'Customs Cleared': 'Loaded Into Container',
      'Submitted': 'Shipment Processing',
      'Under Shipment Processing': 'Shipment In Transit',
      'In Transit': 'Under Processing',
      'Arrived at Facility': 'Arrived at Sort Facility',
      'Ready for Delivery': 'Ready for Delivery',
      'Arrived at Destination': 'Ready for Delivery',
      'Delivered': 'Shipment Delivered'
    };

    const syncedStatus = statusSyncMap[nextStatus] || nextStatus;

    let consignmentEta = null;

    await withTransaction(async (client) => {
      try {
        // 1. Calculate ETA
        if (nextStatus !== 'Delivered') {
          const etaResult = await calculateETA(client, syncedStatus);
          consignmentEta = etaResult.eta;
        } else {
          consignmentEta = new Date().toISOString().split('T')[0];
        }

        // 2. Update consignment
        await client.query(
          'UPDATE consignments SET status = $1, eta = $2, updated_at = NOW() WHERE id = $3',
          [nextStatus, consignmentEta, numericId]
        );

        // 3. Log to consignment_tracking
        await client.query(`
          INSERT INTO consignment_tracking 
            (consignment_id, event_type, old_status, new_status, timestamp, details, created_at, source, action)
          VALUES 
            ($1, $2, $3, $4, NOW(), $5, NOW(), 'api', 'status_advanced')
        `, [
          numericId,
          'status_advanced',
          currentStatus,
          nextStatus,
          JSON.stringify({
            reason: 'Manual advance',
            newEta: consignmentEta,
            syncedTo: syncedStatus,
            user: req.user?.id || 'system'
          })
        ]);

        // 4. Get linked orders
        const orderIdsRes = await client.query(
          'SELECT orders FROM consignments WHERE id = $1',
          [numericId]
        );
        let rawOrders = orderIdsRes.rows[0]?.orders || [];
        if (typeof rawOrders === 'string') rawOrders = JSON.parse(rawOrders || '[]');

        syncOrderIds = rawOrders
          .map(oid => parseInt(oid, 10))
          .filter(oid => !isNaN(oid) && oid > 0);

        if (syncOrderIds.length > 0) {
          // Update orders
          await client.query(
            'UPDATE orders SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE id = ANY($2::int[])',
            [syncedStatus, syncOrderIds]
          );

          // Update receivers
          await client.query(
            `UPDATE receivers 
             SET status = $1, eta = $2, updated_at = CURRENT_TIMESTAMP
             WHERE order_id = ANY($3::int[])`,
            [syncedStatus, consignmentEta, syncOrderIds]
          );

          // Sync containers (with safe parsing)
          const receiversRes = await client.query(
            'SELECT id, containers FROM receivers WHERE order_id = ANY($1::int[])',
            [syncOrderIds]
          );

          for (const recvRow of receiversRes.rows) {
            try {
              let containers = recvRow.containers || [];
              if (typeof containers === 'string') {
                // Handle common bad formats
                containers = containers
                  .replace(/"/g, '')     // remove quotes
                  .replace(/[\[\]]/g, '') // remove brackets
                  .split(',')            // split by comma
                  .map(c => c.trim())
                  .filter(c => c.length > 0);
              } else if (!Array.isArray(containers)) {
                containers = [];
              }

              // Now call your function with cleaned array
              await updateLinkedContainersStatus(client, recvRow.id, syncedStatus, 'system');
            } catch (containerErr) {
              console.warn(`Failed to sync containers for receiver ${recvRow.id}:`, containerErr.message);
              // Continue — don't rollback whole transaction
            }
          }
        }

        // Notification
        try {
          const updated = await client.query('SELECT * FROM consignments WHERE id = $1', [numericId]);
          await sendNotification(updated.rows[0], `status_advanced_to_${nextStatus}`, {
            reason: 'Manual advance',
            syncedOrders: syncOrderIds.length,
            syncedStatus
          });
        } catch (notifErr) {
          console.warn('Notification failed:', notifErr);
        }

      } catch (innerErr) {
        throw innerErr;
      }
    });

    res.json({
      success: true,
      message: `Status advanced to "${nextStatus}"`,
      data: {
        previousStatus: currentStatus,
        newStatus: nextStatus,
        syncedStatus: syncedStatus,
        newEta: consignmentEta,
        affectedOrders: syncOrderIds.length
      }
    });

  } catch (err) {
    console.error("Error advancing status:", err.stack || err);
    res.status(500).json({ error: 'Failed to advance status' });
  }
}


// export async function advanceStatus(req, res) {
//   console.log("Advance Status Request Params:", req.params);    
//   try {
//     const { id } = req.params;
//     const numericId = parseInt(id, 10);
//     if (isNaN(numericId) || numericId <= 0) {
//       return res.status(400).json({ error: 'Invalid consignment ID.' });
//     }

//     const { rows } = await pool.query('SELECT status FROM consignments WHERE id = $1', [numericId]);
//     if (rows.length === 0) {
//       return res.status(404).json({ error: 'Consignment not found' });
//     }

//     const currentStatus = rows[0].status;

//     // Your existing nextStatusMap (keep unchanged)
//     const nextStatusMap = {
//       'Drafts Cleared': 'Submitted On Vessel',
//       'Submitted On Vessel': 'Customs Cleared',
//       'Customs Cleared': 'Submitted',
//       'Submitted': 'Under Shipment Processing',
//       'Under Shipment Processing': 'In Transit',
//       'In Transit': 'Arrived at Facility',
//       'Arrived at Facility': 'Ready for Delivery',
//       'Ready for Delivery': 'Arrived at Destination',
//       'Arrived at Destination': 'Delivered',
//       // ... keep the rest
//     };

//     const nextStatus = nextStatusMap[currentStatus];
//     if (!nextStatus) {
//       return res.status(400).json({ error: `No next status from ${currentStatus}` });
//     }

//     const mapping = CONSIGNMENT_TO_STATUS_MAP[nextStatus];
//     if (!mapping) {
//       console.warn(`No mapping defined for consignment status: ${nextStatus}`);
//     }

//     let updateError = null;

//     await withTransaction(async (client) => {
//       try {
//         // 1. Update consignment status
//         let consignmentEta = null;
//         if (mapping && nextStatus !== 'Delivered') {
//           const etaResult = await calculateETA(client, mapping.shipment);
//           consignmentEta = etaResult.eta;
//         } else if (nextStatus === 'Delivered') {
//           consignmentEta = new Date().toISOString().split('T')[0];
//         }

//         await client.query(
//           'UPDATE consignments SET status = $1, eta = $2, updated_at = NOW() WHERE id = $3',
//           [nextStatus, consignmentEta, numericId]
//         );

//         // 2. Log tracking
//         await safeLogToTracking(client, numericId, 'status_advanced', {
//           from: currentStatus,
//           to: nextStatus,
//           newEta: consignmentEta,
//           reason: 'Manual advance'
//         });

//         // 3. Sync linked orders → receivers → containers
//         if (mapping) {
//           const orderIdsQuery = await client.query('SELECT orders FROM consignments WHERE id = $1', [numericId]);
//           let rawOrders = orderIdsQuery.rows[0]?.orders || [];
//           if (typeof rawOrders === 'string') rawOrders = JSON.parse(rawOrders || '[]');

//           const syncOrderIds = extractOrderIds(rawOrders)
//             .map(oid => parseInt(oid, 10))
//             .filter(oid => !isNaN(oid) && oid > 0);

//           if (syncOrderIds.length > 0) {
//             // Update orders (optional)
//             await client.query(
//               'UPDATE orders SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE id = ANY($2::int[])',
//               [nextStatus, syncOrderIds]
//             );

//             // Update ALL receivers: correct shipment status
//             await client.query(
//               `UPDATE receivers 
//                SET status = $1, updated_at = CURRENT_TIMESTAMP
//                WHERE order_id = ANY($2::int[])`,
//               [mapping.shipment, syncOrderIds]
//             );

//             // Recalculate ETA for each receiver using eta_config table
//             const etaResult = await calculateETA(client, mapping.shipment);

//             await client.query(
//               `UPDATE receivers 
//                SET eta = $1, updated_at = CURRENT_TIMESTAMP
//                WHERE order_id = ANY($2::int[])`,
//               [etaResult.eta, syncOrderIds]
//             );

//             // Sync containers via existing function
//             const receiversResult = await client.query(
//               'SELECT id FROM receivers WHERE order_id = ANY($1::int[])',
//               [syncOrderIds]
//             );

//             for (const row of receiversResult.rows) {
//               await updateLinkedContainersStatus(client, row.id, mapping.shipment, 'system');
//             }
//           }
//         }

//         // Notification (non-critical)
//         try {
//           const updated = await client.query('SELECT * FROM consignments WHERE id = $1', [numericId]);
//           await sendNotification(updated.rows[0], `status_advanced_to_${nextStatus}`, { reason: 'Manual advance' });
//         } catch (notifErr) {
//           console.warn(`Notification failed:`, notifErr);
//         }

//       } catch (updateErr) {
//         updateError = updateErr;
//         if (updateErr.code === '22P02') {
//           console.warn(`Enum violation for '${nextStatus}'`);
//         } else {
//           throw updateErr;
//         }
//       }
//     });

//     if (updateError && updateError.code === '22P02') {
//       return res.status(409).json({ error: `Status '${nextStatus}' not in DB enum.` });
//     }

//     res.json({ 
//       message: `Status advanced to ${nextStatus}`,
//       data: { newStatus: nextStatus, previousStatus: currentStatus }
//     });

//   } catch (err) {
//     console.error("Error advancing status:", err);
//     res.status(500).json({ error: 'Failed to advance status' });
//   }
// }
// Enhanced calculateETA (returns { eta, daysUntil }; uses exact status match from eta_config table)
async function calculateETA(client, status, baseDate = new Date()) {  // Dynamic: Use current date as base
  try {
    const configQuery = `SELECT days_offset FROM eta_config WHERE status = $1`;  // Exact match
    const configResult = await client.query(configQuery, [status]);
    if (configResult.rowCount === 0) {
      console.log(`No ETA config for status: ${status}; using baseDate (0 days)`);
      const eta = baseDate.toISOString().split('T')[0];
      return { eta, daysUntil: 0 };
    }
    const days = configResult.rows[0].days_offset;
    if (status.toLowerCase().includes('delivered')) {  // Simplified check
      const eta = baseDate.toISOString().split('T')[0];
      return { eta, daysUntil: 0 };
    }
    const etaDate = new Date(baseDate.getTime() + days * 86400000);
    const eta = etaDate.toISOString().split('T')[0];
    const daysUntil = computeDaysUntilEta(eta, baseDate);
    console.log(`[calculateETA] For status "${status}": offset=${days} days → ETA=${eta} (days until: ${daysUntil})`);
    return { eta, daysUntil };
  } catch (err) {
    console.error('ETA calc error:', err);
    const eta = new Date().toISOString().split('T')[0];
    return { eta, daysUntil: 0 };
  }
}

// Helper for status color mapping (extended to match eta_config statuses)
function getStatusColor(status) {
  const colors = {
    'Draft': 'info',
    'Submitted': 'warning',
    'In Transit': 'warning',
    'Delivered': 'success',
    'Cancelled': 'error',
    'Created': 'info',
    'Order Created': 'info',
    'Ready for Loading': 'info',
    'Loaded into Container': 'warning',
    'Shipment Processing': 'warning',
    'Shipment In Transit': 'warning',
    'Under Processing': 'warning',
    'Arrived at Sort Facility': 'success',
    'Ready for Delivery': 'success',
    'Shipment Delivered': 'success'
  };
  return colors[status] || 'default';
}
export async function updateContainer(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    const { cid } = req.params;
    const updates = req.body;
    const created_by = updates.created_by || req.user?.id || 'system'; // Use req.user if available
    const today = new Date(); // Dynamic current date
    console.log('Container updates:', { cid, updates, created_by });
    // Fetch current container details (no linked orders until schema supports container_id in receivers/order_items)
    const currentQuery = `
      SELECT
        cm.owner_type, cm.container_number, cm.container_size, cm.container_type, cm.remarks,
        cs.availability as current_availability, cs.location as current_location
      FROM container_master cm
      LEFT JOIN container_status cs ON cm.cid = cs.cid
        AND cs.sid = (SELECT MAX(sid) FROM container_status WHERE cid = cm.cid)
      WHERE cm.cid = $1
    `;
    const currentResult = await client.query(currentQuery, [cid]);
    const current = currentResult.rows[0];
    if (!current) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Container not found' });
    }
    const currentOwnerType = current.owner_type;
    const linkedOrders = []; // TODO: Once schema has linking (e.g., receivers.container_id), restore array_agg join for cascades
    // Prevent changing owner_type
    if (updates.owner_type && updates.owner_type !== currentOwnerType) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'Cannot change owner_type; manual migration required' });
    }
    // If derived_status provided, treat as new availability and trigger cascades
    if (updates.derived_status) {
      updates.availability = updates.derived_status;
    }
    const newAvailability = updates.availability || current.current_availability;
    const newLocation = updates.location || current.current_location;
    // Validate updated fields (only if provided) - unchanged
    const updatedSocFields = {
      location: updates.location,
      manufacture_date: updates.manufacture_date,
      purchase_date: updates.purchase_date,
      purchase_price: updates.purchase_price,
      purchase_from: updates.purchase_from,
      owned_by: updates.owned_by,
      available_at: updates.available_at,
    };
    const updatedCocFields = {
      hire_start_date: updates.hire_start_date,
      hire_end_date: updates.hire_end_date,
      hired_by: updates.hired_by,
      return_date: updates.return_date,
      free_days: updates.free_days,
      place_of_loading: updates.place_of_loading,
      place_of_destination: updates.place_of_destination,
    };
    let updateErrors = [];
    // Validate location if provided - FIXED: Conditional normalization to avoid double underscore
    const validLocations = ['karachi_port', 'dubai_port'];
    if (updates.location) {
      let normalizedLocation = updates.location.toLowerCase().trim().replace(/\s+/g, '_');
      // Only append '_port' if it ends with 'port' without leading '_'
      if (normalizedLocation.endsWith('port') && !normalizedLocation.endsWith('_port')) {
        normalizedLocation = normalizedLocation.replace(/port$/, '_port');
      }
      // Handle any internal 'port' if needed, but avoid over-replacing
      if (!validLocations.includes(normalizedLocation)) {
        updateErrors.push(`location (must be one of: ${validLocations.join(', ')})`);
      } else {
        updates.location = normalizedLocation;  // Normalize for consistency
      }
    }
    if (currentOwnerType === 'soc') {
      for (const [field, value] of Object.entries(updatedSocFields)) {
        if (value !== undefined) {
          if (['manufacture_date', 'purchase_date'].includes(field) && !isValidDate(value)) {
            updateErrors.push(`${field} (got: "${value}")`);
          } else if (!value && !['location', 'available_at'].includes(field)) {
            updateErrors.push(field);
          }
        }
      }
    } else {
      for (const [field, value] of Object.entries(updatedCocFields)) {
        if (value !== undefined) {
          if (['hire_start_date', 'hire_end_date', 'return_date'].includes(field) && !isValidDate(value)) {
            updateErrors.push(`${field} (got: "${value}")`);
          } else if (!value && !['return_date'].includes(field)) {
            updateErrors.push(field);
          } else if (field === 'free_days' && isNaN(value)) {
            updateErrors.push('free_days (must be number)');
          }
        }
      }
    }
    if (updateErrors.length > 0) {
      console.warn('Update validation failed for fields:', updateErrors);
      await client.query('ROLLBACK');
      return res.status(400).json({
        error: 'Invalid update fields',
        details: updateErrors.join(', ')
      });
    }
    // Check for duplicate container_number if updating it
    if (updates.container_number) {
      const checkQuery = 'SELECT cid FROM container_master WHERE container_number = $1 AND status = 1 AND cid != $2';
      const checkResult = await client.query(checkQuery, [updates.container_number, cid]);
      if (checkResult.rowCount > 0) {
        await client.query('ROLLBACK');
        return res.status(409).json({ error: 'Container number already exists' });
      }
    }
    // Update master (core fields) - unchanged
    const masterKeys = ['container_number', 'container_size', 'container_type', 'remarks'];
    const masterUpdates = Object.keys(updates).filter(key => masterKeys.includes(key));
    if (masterUpdates.length > 0) {
      const setClause = masterUpdates.map((key, index) => `${key} = $${index + 1}`).join(', ');
      const values = masterUpdates.map(key => updates[key]);
      values.push(cid);
      const updateQuery = `UPDATE container_master SET ${setClause}, updated_at = CURRENT_TIMESTAMP WHERE cid = $${values.length}`;
      await client.query(updateQuery, values);
    }
    // Insert new status history entry for availability/location changes
    if (updates.availability !== undefined || updates.location !== undefined) {
      let columns = ['cid'];
      let placeholders = ['$1'];
      let qvalues = [cid];
      let notes = 'Status updated';
      let paramIndex = qvalues.length + 1;
      let currentPhysicalLocation = current.current_location;  // From fetch
      if (updates.location) {
        currentPhysicalLocation = updates.location;  // Validated above
      }
      if (updates.availability !== undefined) {
        columns.push('availability');
        placeholders.push(`$${paramIndex}`);
        qvalues.push(updates.availability);
        notes += ` availability to ${updates.availability}`;
        paramIndex++;
      }
      if (updates.location !== undefined) {
        columns.push('location');
        placeholders.push(`$${paramIndex}`);
        qvalues.push(currentPhysicalLocation);  // Use validated
        notes += ` location to ${currentPhysicalLocation}`;
        paramIndex++;
      } else if (updates.availability !== undefined) {
        columns.push('location');
        placeholders.push(`$${paramIndex}`);
        qvalues.push(currentPhysicalLocation);  // Preserve prior
        paramIndex++;
      }
      columns.push('status_notes');
      placeholders.push(`$${paramIndex}`);
      qvalues.push(notes + ` from ${currentPhysicalLocation.toUpperCase()}`);
      paramIndex++;
      columns.push('created_by');
      placeholders.push(`$${paramIndex}`);
      qvalues.push(created_by);
      const insertQuery = `INSERT INTO container_status (${columns.join(', ')}) VALUES (${placeholders.join(', ')})`;
      await client.query(insertQuery, qvalues);
    }
    // Conditional: Update purchase (SOC) or hire (COC) - unchanged, but add updated_at
    const effectiveOwnerType = updates.owner_type || currentOwnerType;
    if (effectiveOwnerType === 'soc') {
      const purchaseKeys = ['manufacture_date', 'purchase_date', 'purchase_price', 'purchase_from', 'owned_by', 'available_at', 'currency'];
      const purchaseUpdates = Object.keys(updates).filter(key => purchaseKeys.includes(key));
      if (purchaseUpdates.length > 0) {
        const normManufactureDate = normalizeDate(updates.manufacture_date);
        const normPurchaseDate = normalizeDate(updates.purchase_date);
        const normAvailableAt = updates.available_at;
        const normalizedUpdates = {
          ...updates,
          manufacture_date: normManufactureDate,
          purchase_date: normPurchaseDate,
          available_at: normAvailableAt
        };
        const setClause = purchaseUpdates.map((key, index) => `${key} = $${index + 1}`).join(', ');
        const values = purchaseUpdates.map(key => normalizedUpdates[key]);
        values.push(cid);
        const updateQuery = `UPDATE container_purchase_details SET ${setClause}, updated_at = CURRENT_TIMESTAMP WHERE cid = $${values.length}`;
        console.log('Updating SOC with normalized dates:', { manufacture_date: normManufactureDate, purchase_date: normPurchaseDate, available_at: normAvailableAt });
        await client.query(updateQuery, values);
      }
    } else { // COC (hired)
      const hireKeys = ['hire_start_date', 'hire_end_date', 'hired_by', 'return_date', 'free_days', 'place_of_loading', 'place_of_destination'];
      const hireUpdates = Object.keys(updates).filter(key => hireKeys.includes(key));
      if (hireUpdates.length > 0) {
        const normHireStartDate = normalizeDate(updates.hire_start_date);
        const normHireEndDate = normalizeDate(updates.hire_end_date);
        const normReturnDate = normalizeDate(updates.return_date);
        const normalizedUpdates = {
          ...updates,
          hire_start_date: normHireStartDate,
          hire_end_date: normHireEndDate,
          return_date: normReturnDate
        };
        const setClause = hireUpdates.map((key, index) => `${key} = $${index + 1}`).join(', ');
        const values = hireUpdates.map(key => normalizedUpdates[key]);
        values.push(cid);
        const updateQuery = `UPDATE container_hire_details SET ${setClause}, updated_at = CURRENT_TIMESTAMP WHERE cid = $${values.length}`;
        console.log('Updating COC with normalized dates:', { hire_start_date: normHireStartDate, hire_end_date: normHireEndDate, return_date: normReturnDate });
        await client.query(updateQuery, values);
      }
    }
    // NEW: Cascade to linked orders and receivers based on new availability/status
    // (Skipped until linkedOrders is populated via schema update)
    if (newAvailability && linkedOrders.length > 0) {
      for (const linkedOrder of linkedOrders) {
        const orderId = linkedOrder.order_id;
        const currentOrderEta = linkedOrder.order_eta; // From enhanced query
        const receivers = linkedOrder.receivers || [];
        // Map container statuses to receiver/order statuses (customize based on your workflow; consistent casing)
        let newReceiverStatus = null;
        const lowerAvailability = newAvailability.toLowerCase();
        switch (lowerAvailability) {
          case 'available':
            newReceiverStatus = 'Ready for Loading'; // Early stage
            break;
          case 'hired':
          case 'occupied':
            newReceiverStatus = 'Loaded Into Container'; // Mid stage
            break;
          case 'in transit':
            newReceiverStatus = 'Shipment In Transit';
            break;
          case 'loaded':
            newReceiverStatus = 'Loaded Into Container';
            break;
          case 'arrived':
            newReceiverStatus = 'Arrived at Sort Facility';
            break;
          case 'de-linked':
          case 'returned':
          case 'cleared':
            newReceiverStatus = 'Ready for Delivery'; // Late stage
            break;
          case 'under repair':
            newReceiverStatus = 'Under Processing'; // Hold
            break;
          default:
            newReceiverStatus = null;
        }
        if (newReceiverStatus) {
          // Update all receivers for this order to new status (or specific logic if multi-container)
          for (const receiver of receivers) {
            const receiverId = receiver.id;
            // Use similar logic as updateReceiverStatus: update receiver, recalc ETA, cascade to order
            const receiverUpdateQuery = `
              UPDATE receivers SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2 AND order_id = $3
            `;
            await client.query(receiverUpdateQuery, [newReceiverStatus, receiverId, orderId]);
            // Recalc ETA for receiver if status changed
            const etaResult = await calculateETA(client, newReceiverStatus, today);
            await client.query(`UPDATE receivers SET eta = $1 WHERE id = $2`, [etaResult.eta, receiverId]);
            // Log tracking
            await client.query(`
              INSERT INTO order_tracking (order_id, receiver_id, status, old_status, created_by, created_time)
              VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
            `, [orderId, receiverId, newReceiverStatus, receiver.status, created_by]);
            console.log(`Cascaded container status "${newAvailability}" to receiver ${receiverId}: ${newReceiverStatus}`);
          }
          // Cascade to overall order status (reuse your updateOrderOverallStatus)
          const refreshedReceiversQuery = `SELECT id, status, eta FROM receivers WHERE order_id = $1`;
          const refreshedReceivers = (await client.query(refreshedReceiversQuery, [orderId])).rows;
          await updateOrderOverallStatus(client, orderId, newReceiverStatus, refreshedReceivers);
          // Update order ETA to min receiver ETA
          const minEtaQuery = `SELECT MIN(eta) as min_eta FROM receivers WHERE order_id = $1`;
          const minEtaResult = await client.query(minEtaQuery, [orderId]);
          const orderNewEta = minEtaResult.rows[0].min_eta;
          if (orderNewEta && orderNewEta !== currentOrderEta) { // Fixed: Compare to order_eta
            await client.query(`UPDATE orders SET eta = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`, [orderNewEta, orderId]);
            console.log(`Updated order ${orderId} ETA due to container cascade: ${orderNewEta}`);
          }
        }
        // Update order_items consignment_status if applicable (use mapped status if available, else availability)
        // TODO: Uncomment/adjust if order_items has container_id or alternative linking (e.g., via receiver_id)
        // const consignmentStatus = newReceiverStatus || newAvailability;
        // await client.query(`
        //   UPDATE order_items SET consignment_status = $1, updated_at = CURRENT_TIMESTAMP
        //   WHERE order_id = $2 AND container_id = $3
        // `, [consignmentStatus, orderId, cid]);
      }
    }
    await client.query('COMMIT');
    console.log(`Updated container ${cid} with cascades to ${linkedOrders.length} orders`);
    res.json({
      message: 'Container updated successfully',
      cascades: { affected_orders: linkedOrders.length }
    });
  } catch (err) {
    if (client) {
      await client.query('ROLLBACK');
    }
    console.error("Container update error:", err.message);
    if (err.code === '23505') {
      return res.status(409).json({ error: 'Container number already exists' });
    }
    if (err.code === '23514') {
      return res.status(400).json({ error: 'Invalid value for constrained field (e.g., owner_type or availability)' });
    }
    if (err.code === '22007' || err.code === '22008') {
      return res.status(400).json({ error: 'Invalid date format. Use YYYY-MM-DD.' });
    }
    res.status(500).json({ error: err.message || 'Failed to update container' });
  } finally {
    if (client) {
      client.release();
    }
  }
}
// Helper for order-specific status color (if separate; harmonized with above)
function getOrderStatusColor(status) {
  return getStatusColor(status);  // Reuse for consistency
}

// NEW: Reverse mapping for container status to receiver status (for cascades)
function mapContainerStatusToReceiverStatus(containerStatus) {
  const mapping = {
    'Available': 'Ready for Loading',
    'Loaded': 'Loaded Into Container',
    'In Transit': 'Shipment In Transit',
    'Arrived': 'Arrived at Sort Facility',
    'De-Linked': 'Ready for Delivery',
    'Returned': 'Shipment Delivered',
    'Under Repair': 'Under Processing',
    'Hired': 'Loaded Into Container',
    'Occupied': 'Loaded Into Container',
    'Cleared': 'Shipment Delivered'
  };
  return mapping[containerStatus] || null;  // Null if no direct map
}

// Updated validation helper (case-insensitive, trimmed)
function isValidReceiverStatus(status) {
  if (!status) return false;
  const trimmed = status.trim();
  const validStatuses = [
    'Ready for Loading', 'Loaded Into Container', 'Shipment Processing',
    'Shipment In Transit', 'Under Processing', 'Arrived at Sort Facility',
    'Ready for Delivery', 'Shipment Delivered'
  ];
  return validStatuses.some(valid => valid.toLowerCase() === trimmed.toLowerCase());
}

export async function updateReceiverStatus(req, res) {
  let client;
  try {
    const orderId = req.params.orderId;
    const receiverId = req.params.id;
    const { status, notifyClient = true, notifyParties = false, forceRecalcEta = false } = req.body || {};
    const created_by = req.user?.id || 'system';
    console.log('Received request to update receiver status:', { orderId, receiverId }, { status, notifyClient, notifyParties, forceRecalcEta });
     const validStatuses = [
      'Ready for Loading', 'Loaded Into Container', 'Shipment Processing',
      'Shipment In Transit', 'Under Processing', 'Arrived at Sort Facility',
      'Ready for Delivery', 'Shipment Delivered'
    ];
    // Enhanced validation with logging and case-insensitivity
    if (!orderId || isNaN(parseInt(orderId))) {
      return res.status(400).json({ error: 'Valid order ID is required' });
    }
    if (!receiverId || isNaN(parseInt(receiverId))) {
      return res.status(400).json({ error: 'Valid receiver ID is required' });
    }
    const trimmedStatus = (status || '').trim();
    if (!trimmedStatus || !isValidReceiverStatus(trimmedStatus)) {
      console.log('Invalid status provided:', trimmedStatus);  // Debug log
      const validStatuses = [
        'Ready for Loading', 'Loaded Into Container', 'Shipment Processing',
        'Shipment In Transit', 'Under Processing', 'Arrived at Sort Facility',
        'Ready for Delivery', 'Shipment Delivered'
      ];
      return res.status(400).json({ 
        error: 'Valid status is required', 
        validStatuses,
        details: trimmedStatus ? `Received: "${trimmedStatus}" (case-insensitive match failed)` : 'No status provided'
      });
    }
    // Normalize to exact casing
    const normalizedStatus = validStatuses.find(valid => valid.toLowerCase() === trimmedStatus.toLowerCase());
    
    client = await pool.connect();
    await client.query('BEGIN');
    
    // Fetch order, receiver, and ALL receivers (unchanged)
    const detailsQuery = `
      SELECT o.*, s.sender_email, s.sender_contact,
             r.id as receiver_id, r.receiver_name, r.receiver_email, r.receiver_contact, r.status as receiver_status, r.eta, r.total_weight, r.containers
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
    
    const allReceiversQuery = `SELECT id, status, eta FROM receivers WHERE order_id = $1`;
    const allRecResult = await client.query(allReceiversQuery, [parseInt(orderId)]);
    let allReceivers = allRecResult.rows;
    
    // Update receiver status (use normalized)
    const updateQuery = `
      UPDATE receivers
      SET status = $1, updated_at = CURRENT_TIMESTAMP
      WHERE id = $2 AND order_id = $3
      RETURNING id, status, eta, containers
    `;
    const updateResult = await client.query(updateQuery, [normalizedStatus, parseInt(receiverId), parseInt(orderId)]);
    if (updateResult.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(500).json({ error: 'Failed to update receiver status' });
    }
    let updatedReceiver = updateResult.rows[0];
    let daysUntilEta = computeDaysUntilEta(updatedReceiver.eta);
    let finalStatus = normalizedStatus;
    
    // Auto-upgrade if past ETA (unchanged)
    if (daysUntilEta !== null && daysUntilEta <= 0 && finalStatus !== 'Shipment Delivered') {
      finalStatus = 'Shipment Delivered';
      await client.query(`UPDATE receivers SET status = $1 WHERE id = $2`, [finalStatus, parseInt(receiverId)]);
      console.log(`Auto-upgraded receiver ${receiverId} to ${finalStatus} (past ETA: ${daysUntilEta} days)`);
      const refetchResult = await client.query(`SELECT id, status, eta, containers FROM receivers WHERE id = $1`, [parseInt(receiverId)]);
      updatedReceiver = refetchResult.rows[0];
      daysUntilEta = computeDaysUntilEta(updatedReceiver.eta);
    }
    
    // Dynamically fetch offsets (unchanged)
    const oldOffsetQuery = `SELECT days_offset FROM eta_config WHERE status = $1 LIMIT 1`;
    const oldOffsetResult = await client.query(oldOffsetQuery, [oldStatus || 'In Process']);
    const oldOffset = oldOffsetResult.rowCount > 0 ? oldOffsetResult.rows[0].days_offset : Infinity;
    
    const newOffsetQuery = `SELECT days_offset FROM eta_config WHERE status = $1 LIMIT 1`;
    const newOffsetResult = await client.query(newOffsetQuery, [finalStatus]);
    const newOffset = newOffsetResult.rowCount > 0 ? newOffsetResult.rows[0].days_offset : 0;
    
    const statusAdvanced = newOffset < oldOffset;
    
    // Recalculate ETA (unchanged)
    let newEta = updatedReceiver.eta;
    if (!updatedReceiver.eta || forceRecalcEta || statusAdvanced) {
      const etaResult = await calculateETA(client, finalStatus);
      newEta = etaResult.eta;
      if (newEta !== updatedReceiver.eta) {
        await client.query(`UPDATE receivers SET eta = $1 WHERE id = $2`, [newEta, parseInt(receiverId)]);
        console.log(`Recalculated ETA for receiver ${receiverId} (status: ${finalStatus}): ${newEta} (days until: ${etaResult.daysUntil})`);
        const refetchResult = await client.query(`SELECT id, status, eta, containers FROM receivers WHERE id = $1`, [parseInt(receiverId)]);
        updatedReceiver = refetchResult.rows[0];
        daysUntilEta = etaResult.daysUntil;
      }
    }
    
    // Update allReceivers
    allReceivers = allReceivers.map(r =>
      r.id === parseInt(receiverId)
        ? { ...r, status: finalStatus, eta: newEta }
        : r
    );
    
    // Cascade to linked containers (bidirectional)
    await updateLinkedContainersStatus(client, parseInt(receiverId), finalStatus, created_by);
    
    // Cascade: Update order_items
    await client.query(`
      UPDATE order_items
      SET consignment_status = $1, updated_at = CURRENT_TIMESTAMP
      WHERE receiver_id = $2
    `, [finalStatus, parseInt(receiverId)]);
    
    // Cascade: Update overall order status
    await updateOrderOverallStatus(client, parseInt(orderId), finalStatus, allReceivers);
    
    // Recalc and update order-level ETA
    const minEtaQuery = `SELECT MIN(eta) as min_eta FROM receivers WHERE order_id = $1`;
    const minEtaResult = await client.query(minEtaQuery, [parseInt(orderId)]);
    const orderNewEta = minEtaResult.rows[0].min_eta;
    if (orderNewEta && orderNewEta !== order.eta) {
      await client.query(`UPDATE orders SET eta = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`, [orderNewEta, parseInt(orderId)]);
      console.log(`Updated order ${orderId} ETA to earliest receiver: ${orderNewEta}`);
    }
    
    // Log to order_tracking
    await client.query(`
      INSERT INTO order_tracking (order_id, receiver_id, status, old_status, created_by, created_time)
      VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
    `, [parseInt(orderId), parseInt(receiverId), finalStatus, oldStatus, created_by]);
    
    await client.query('COMMIT');
    
    res.status(200).json({
      success: true,
      message: `Receiver status updated to "${finalStatus}". ETA recalculated to "${newEta}". Cascades (incl. containers) and notifications triggered.`,
      updatedReceiver: {
        id: updatedReceiver.id,
        status: finalStatus,
        eta: newEta,
        days_until_eta: daysUntilEta,
        containers: updatedReceiver.containers
      }
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



function mapReceiverStatusToContainerStatus(receiverStatus) {
  const map = {
    'Shipment Processing': 'Shipment Processing',
    'Shipment In Transit': 'In Transit',
    'Ready for Delivery': 'Ready for Delivery',
    'Under Processing': 'Under Processing',
    'Loaded Into Container': 'Loaded',
    'Ready for Loading': 'Ready for Loading',
    'Order Created': 'Created',
    'Created': 'Created',
    'Arrived at Sort Facility': 'Arrived at Sort Facility',
    'Shipment Delivered': 'Delivered',
  };
  return map[receiverStatus] || receiverStatus; // fallback
}

// NEW: Helper to update container status based on receiver status (insert new history entry)
async function updateLinkedContainersStatus(client, receiverId, newReceiverStatus, created_by) {
  try {
    // Fetch receiver's containers (JSON array)
    const receiverQuery = `SELECT containers FROM receivers WHERE id = $1`;
    const recResult = await client.query(receiverQuery, [receiverId]);
    if (recResult.rowCount === 0 || !recResult.rows[0].containers) {
      console.log(`No containers linked to receiver ${receiverId}`);
      return;
    }

    const containersJson = recResult.rows[0].containers;
    let containers = [];
    try {
      containers = JSON.parse(containersJson);
      if (!Array.isArray(containers)) containers = [];
    } catch (parseErr) {
      console.warn(`Failed to parse containers JSON for receiver ${receiverId}:`, parseErr);
      return;
    }

    if (containers.length === 0) return;

    // Get container CIDs from numbers
    const cidQuery = `SELECT cid FROM container_master WHERE container_number = ANY($1)`;
    const cidResult = await client.query(cidQuery, [containers]);
    const cids = cidResult.rows.map(row => row.cid);

    if (cids.length === 0) return;

    // Map to new container status
    const newContainerStatus = mapReceiverStatusToContainerStatus(newReceiverStatus);
    const location = `Receiver ${receiverId} (Status: ${newReceiverStatus})`;

    // Insert history for each linked container
    const insertQuery = `
      INSERT INTO container_status (cid, availability, location, status_notes, created_by)
      VALUES ($1, $2, $3, $4, $5)
    `;
    for (const cid of cids) {
      await client.query(insertQuery, [cid, newContainerStatus, location, `Synced from receiver status change`, created_by]);
      console.log(`Updated container status for CID ${cid} to "${newContainerStatus}" due to receiver ${receiverId} → ${newReceiverStatus}`);
    }
  } catch (err) {
    console.error(`Error updating linked containers for receiver ${receiverId}:`, err);
    // Don't throw; non-critical cascade
  }
}

async function updateOrderOverallStatus(client, orderId, newReceiverStatus, receivers) {  // Pass receivers for efficiency
  try {
    // Dynamically fetch status order from eta_config (sorted by days_offset DESC: early stages first, late last)
    const configQuery = `
      SELECT DISTINCT status, days_offset 
      FROM eta_config 
      WHERE status NOT IN ('Cancelled')  -- Exclude special cases
      ORDER BY days_offset DESC
    `;
    const configResult = await client.query(configQuery);
    const statusesFromDb = configResult.rows;
    
    // Build dynamic statusOrder: higher index = later stage (lower offset)
    const statusOrder = {};
    statusesFromDb.forEach((row, index) => {
      statusOrder[row.status] = index;
    });
    
    // Fallback for missing statuses
    const fallbackStatuses = [
      'Created', 'Order Created', 'In Process', 'Submitted', 'In Transit'
    ];
    fallbackStatuses.forEach((status, index) => {
      if (!(status in statusOrder)) {
        statusOrder[status] = fallbackStatuses.length + index;  // Ensure fallbacks come after DB statuses
      }
    });
    
    const receiverStatuses = receivers.map(r => r.status);
    const maxIndex = Math.max(...receiverStatuses.map(s => statusOrder[s] || 0));
    let overallStatus = Object.keys(statusOrder).find(key => statusOrder[key] === maxIndex) || 'In Process';

    // Enhanced: Eta-based auto-upgrade (all past eta → 'Shipment Delivered' if not cancelled)
    const today = new Date();  // Dynamic: Use current date
    const allPastEta = receivers.every(r => {
      if (['Shipment Delivered', 'Cancelled'].includes(r.status)) return false;
      const days = computeDaysUntilEta(r.eta, today);
      return days !== null && days <= 0;
    });
    if (allPastEta && !receiverStatuses.includes('Cancelled')) {
      overallStatus = 'Shipment Delivered';
    }

    // Weighted: e.g., >50% delivered → 'Shipment In Transit' (extend as needed)
    const deliveredPct = (receiverStatuses.filter(s => s === 'Shipment Delivered').length / receivers.length) * 100;
    if (deliveredPct > 50 && overallStatus !== 'Shipment Delivered') {
      overallStatus = 'Shipment In Transit';
    }

    const orderUpdateQuery = `
      UPDATE orders SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2 RETURNING id, status
    `;
    const orderResult = await client.query(orderUpdateQuery, [overallStatus, orderId]);
    if (orderResult.rowCount > 0) {
      console.log(`Cascaded order status to: ${overallStatus} for order ${orderId} (delivered %: ${deliveredPct.toFixed(0)})`);
    }
  } catch (err) {
    console.error('Error updating overall order status:', err);
    throw err;  // Re-throw to handle in caller if needed
  }
}
// Stub functions (define these if missing)
async function calculateETAAll(client, status) {
  // Placeholder: Return { eta: '2025-12-23', daysUntil: 8 }
  return { eta: new Date(Date.now() + 8 * 24 * 60 * 60 * 1000).toISOString().split('T')[0], daysUntil: 8 };
}
function computeDaysUntilEtaAll(etaStr) {
  const eta = new Date(etaStr);
  const now = new Date();
  return Math.max(0, Math.ceil((eta - now) / (24 * 60 * 60 * 1000)));
}



export async function assignContainersToOrdersAll(req, res) {
  console.log('Received request to assign containers to orders', req.body);
  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    const updates = req.body || {};
    const created_by = req.user?.id || 'system';

    let assignments = updates.assignments || updates;
    if (!assignments || typeof assignments !== 'object') {
      assignments = {}; // Ensure it's an object even if invalid
    }

    let assignmentOrderIds = Object.keys(assignments).filter(k => !isNaN(parseInt(k))).map(k => parseInt(k));
    let explicitOrderIds = (updates.order_ids || updates.orderIds || []).filter(id => !isNaN(parseInt(id)));
    let orderIds = [...new Set([...assignmentOrderIds, ...explicitOrderIds])].filter(id => id > 0);

    console.log('Assign containers body:', JSON.stringify(updates, null, 2));
    console.log('Extracted orderIds:', orderIds);

    if (orderIds.length === 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'orderIds array is required and must not be empty' });
    }

    const allContainerIds = new Set();
    let parsedCount = 0;
    const resolvedAssignments = JSON.parse(JSON.stringify(assignments));
    for (const [orderIdStr, orderAssign] of Object.entries(assignments)) {
      if (typeof orderAssign !== 'object') continue;
      console.log(`Parsing order ${orderIdStr}: ${Object.keys(orderAssign).length} receivers`);
      for (const [recIdStr, recAssign] of Object.entries(orderAssign)) {
        if (typeof recAssign !== 'object' || recAssign === null) continue;
        console.log(`  Parsing receiver ${recIdStr}: ${Object.keys(recAssign).length} details`);
        for (const [detailIdxStr, detailAssign] of Object.entries(recAssign)) {
          if (detailAssign && typeof detailAssign === 'object') {
            console.log(`    Detail ${detailIdxStr}: qty=${detailAssign.qty}, containers=${JSON.stringify(detailAssign.containers)}`);
            if (Array.isArray(detailAssign.containers)) {
              detailAssign.containers.forEach((contIdStr) => {
                allContainerIds.add(String(contIdStr));
                parsedCount++;
                console.log(`      Added container ID "${contIdStr}"`);
              });
            } else {
              console.warn(`    No valid containers array in detail ${detailIdxStr}`);
            }
          }
        }
      }
    }
    console.log(`Total unique container IDs extracted for specific: ${allContainerIds.size} (parsed ${parsedCount} items)`);

    const resolvedCids = new Set();
    let fullContainerMap = new Map();
    let containerIdToCidMap = new Map();
    const singleContainerStr = updates.container_number || updates.container_id || updates.cid;
    let singleMode = false;
    let singleCid = null;
    let singleContNum = null;
    let singleFullCont = null;
    let singlePriorLocation = 'karachi_port';
    let singlePreviousStatus = 'Available';

    if (allContainerIds.size > 0) {
      // Original specific handling
      const stringIds = Array.from(allContainerIds).filter(id => isNaN(parseInt(id)));
      const numericIds = Array.from(allContainerIds).filter(id => !isNaN(parseInt(id))).map(id => parseInt(id));
      const containerQuery = `
        SELECT 
          cm.*,
          cs.location as status_location,
          cs.availability as current_availability,
          chd.hire_start_date,
          chd.hire_end_date,
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
        LEFT JOIN container_hire_details chd ON cm.cid = chd.cid
        LEFT JOIN (
          SELECT DISTINCT ON (cid) cid, location, availability
          FROM container_status
          ORDER BY cid, sid DESC NULLS LAST
        ) cs ON cm.cid = cs.cid
        WHERE (cm.container_number = ANY($1::text[]) OR cm.cid = ANY($2::int[]))
          AND (
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
          ) IN ('Available', 'Assigned to Job') AND cs.location IN ('karachi_port', 'dubai_port')
      `;
      const containerResult = await client.query(containerQuery, [stringIds, numericIds]);
      console.log('Container query for specific IDs', Array.from(allContainerIds), 'returned', containerResult.rows.length, 'rows');
      fullContainerMap = new Map(containerResult.rows.map(row => [row.cid, row]));
      containerResult.rows.forEach(row => {
        containerIdToCidMap.set(row.container_number, row.cid);
        containerIdToCidMap.set(String(row.cid), row.cid);
      });
      const missingIds = Array.from(allContainerIds).filter(contId => !containerIdToCidMap.has(String(contId)));
      if (missingIds.length > 0) {
        console.warn(`Skipping unavailable specific containers: ${missingIds.join(', ')}`);
      }
      if (containerResult.rows.length === 0 && allContainerIds.size > 0) {
        await client.query('ROLLBACK');
        return res.status(400).json({ error: 'No valid available containers specified in assignments' });
      }
      // Resolve specific
      let totalSkippedInAssignments = 0;
      for (const [orderIdStr, orderAssign] of Object.entries(resolvedAssignments)) {
        for (const [recIdStr, recAssign] of Object.entries(orderAssign)) {
          for (const [detailIdxStr, detailAssign] of Object.entries(recAssign)) {
            if (Array.isArray(detailAssign.containers)) {
              const originalCount = detailAssign.containers.length;
              detailAssign.containers = detailAssign.containers
                .map(contIdStr => {
                  const cid = containerIdToCidMap.get(String(contIdStr));
                  if (cid !== undefined) {
                    resolvedCids.add(cid);
                  }
                  return cid;
                })
                .filter(cid => cid !== undefined);
              const skippedInThis = originalCount - detailAssign.containers.length;
              totalSkippedInAssignments += skippedInThis;
              if (detailAssign.containers.length === 0) {
                delete recAssign[detailIdxStr];
              }
            }
          }
          if (Object.keys(recAssign).length === 0) {
            delete orderAssign[recIdStr];
          }
        }
        if (Object.keys(orderAssign).length === 0) {
          delete resolvedAssignments[orderIdStr];
        }
      }
      console.log(`Skipped ${totalSkippedInAssignments} specific container assignments due to unavailability`);
    }

    // Check for single mode from explicit single or from resolved single
    let isSingleFromExplicit = false;
    if (singleContainerStr) {
      isSingleFromExplicit = true;
      const stringIds = [singleContainerStr];
      const numericIds = isNaN(parseInt(singleContainerStr)) ? [] : [parseInt(singleContainerStr)];
      const containerQuery = `
        SELECT 
          cm.*,
          cs.location as status_location,
          cs.availability as current_availability,
          chd.hire_start_date,
          chd.hire_end_date,
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
        LEFT JOIN container_hire_details chd ON cm.cid = chd.cid
        LEFT JOIN (
          SELECT DISTINCT ON (cid) cid, location, availability
          FROM container_status
          ORDER BY cid, sid DESC NULLS LAST
        ) cs ON cm.cid = cs.cid
        WHERE (cm.container_number = ANY($1::text[]) OR cm.cid = ANY($2::int[]))
          AND (
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
          ) IN ('Available', 'Assigned to Job') AND cs.location IN ('karachi_port', 'dubai_port')
      `;
      const containerResult = await client.query(containerQuery, [stringIds, numericIds]);
      if (containerResult.rows.length === 0 || !containerResult.rows[0].derived_status || !['Available', 'Assigned to Job'].includes(containerResult.rows[0].derived_status)) {
        await client.query('ROLLBACK');
        return res.status(400).json({ error: 'Selected container is not available or not in eligible status' });
      }
      singleFullCont = containerResult.rows[0];
      singleCid = singleFullCont.cid;
      singleContNum = singleFullCont.container_number;
      resolvedCids.add(singleCid);
      fullContainerMap = new Map([[singleCid, singleFullCont]]);
      containerIdToCidMap.set(singleContNum, singleCid);
      containerIdToCidMap.set(String(singleCid), singleCid);

      // Fetch prior location
      const priorQuerySingle = `
        SELECT DISTINCT ON (cid) cid, location
        FROM container_status 
        WHERE cid = $1 ORDER BY cid, created_time DESC
      `;
      const priorResult = await client.query(priorQuerySingle, [singleCid]);
      singlePriorLocation = priorResult.rows[0]?.location || 'karachi_port';

      // Fetch previous status
      const prevStatusQuery = `
        SELECT availability as status
        FROM container_status
        WHERE cid = $1
        ORDER BY created_time DESC
        LIMIT 1
      `;
      const prevStatusResult = await client.query(prevStatusQuery, [singleCid]);
      singlePreviousStatus = prevStatusResult.rows[0]?.status || 'Available';
    }

    // Set singleMode if only one container resolved
    if (resolvedCids.size === 1) {
      singleMode = true;
      singleCid = Array.from(resolvedCids)[0];
      singleFullCont = fullContainerMap.get(singleCid);
      singleContNum = singleFullCont.container_number;
      // Fetch prior if not set
      if (!isSingleFromExplicit) {
        const priorQuerySingle = `
          SELECT DISTINCT ON (cid) cid, location
          FROM container_status 
          WHERE cid = $1 ORDER BY cid, created_time DESC
        `;
        const priorResult = await client.query(priorQuerySingle, [singleCid]);
        singlePriorLocation = priorResult.rows[0]?.location || 'karachi_port';

        const prevStatusQuery = `
          SELECT availability as status
          FROM container_status
          WHERE cid = $1
          ORDER BY created_time DESC
          LIMIT 1
        `;
        const prevStatusResult = await client.query(prevStatusQuery, [singleCid]);
        singlePreviousStatus = prevStatusResult.rows[0]?.status || 'Available';
      }
      console.log(`Detected single container mode for container ${singleContNum}, will assign to all remaining details`);
    } else if (resolvedCids.size > 1 || allContainerIds.size > 1) {
      singleMode = false;
    }

    // Batched: Fetch prior physical locations for resolved CIDs (for specific multi)
    const priorQuery = `
      SELECT DISTINCT ON (cid) cid, location
      FROM container_status 
      WHERE cid = ANY($1) ORDER BY cid, created_time DESC
    `;
    let priorLocations = new Map();
    if (resolvedCids.size > 0 && !singleMode) {
      const priorResult = await client.query(priorQuery, [Array.from(resolvedCids)]);
      priorLocations = new Map(priorResult.rows.map(row => [row.cid, row.location || 'karachi_port']));
    } else if (singleMode) {
      priorLocations = new Map([[singleCid, singlePriorLocation]]);
    }

    // Track updates
    const updatedOrders = [];
    const trackingData = [];
    const orderAssignedQtys = new Map();
    const currentOrders = {};
    const currentReceiversByOrder = {};
    for (const orderId of orderIds) {
      const orderQuery = `
        SELECT id, booking_ref, status as overall_status, total_assigned_qty, eta 
        FROM orders 
        WHERE id = $1
      `;
      const orderResult = await client.query(orderQuery, [orderId]);
      if (orderResult.rowCount === 0) continue;
      currentOrders[orderId] = orderResult.rows[0];
      const receiversQuery = `
        SELECT id, receiver_name, containers, qty_delivered, eta, etd, status, total_weight, total_number 
        FROM receivers 
        WHERE order_id = $1
      `;
      const receiversResult = await client.query(receiversQuery, [orderId]);
      currentReceiversByOrder[orderId] = receiversResult.rows;
    }

    const logQuery = `
      INSERT INTO container_assignment_history (
        cid, container_number, order_id, receiver_id, detail_id, assigned_qty, status, action_type, changed_by, notes, previous_status
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    `;

    const statusInsertQuery = `
      INSERT INTO container_status (cid, availability, location, status_notes, created_by, created_time) 
      VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
    `;

    const skippedDetails = [];
    const currentAssignedQuery = `
      SELECT COALESCE(SUM(assigned_qty), 0) as current_assigned
      FROM container_assignment_history 
      WHERE detail_id = $1
    `;

    // Helper function for assigning to a detail (with availability check) - for specific multi
    async function assignToDetail(orderId, recId, detailId, qty, cids, fullContainerMap, priorLocations, receiverEta = null) {
      try {
        const orderItemQuery = `
          SELECT total_number, weight
          FROM order_items 
          WHERE id = $1
        `;
        const orderItemResult = await client.query(orderItemQuery, [detailId]);
        if (orderItemResult.rowCount === 0) return { assigned: 0, newContNumbers: new Set(), logs: [], statusInserts: [] };
        const orderItem = orderItemResult.rows[0];
        const totalNumber = parseInt(orderItem.total_number) || 0;
        const detailWeight = parseFloat(orderItem.weight) || 0;
        const currentAssignedResult = await client.query(currentAssignedQuery, [detailId]);
        const currentAssigned = parseInt(currentAssignedResult.rows[0].current_assigned) || 0;
        const remainingBefore = totalNumber - currentAssigned;
        let detailQty = Math.min(qty, remainingBefore);
        if (detailQty <= 0) {
          console.warn(`No remaining qty for detail ${detailId}`);
          return { assigned: 0, newContNumbers: new Set(), logs: [], statusInserts: [] };
        }
        const totalAssigned = currentAssigned + detailQty;
        const remainingItems = totalNumber - totalAssigned;
        const assignedDetailWeight = detailWeight * (detailQty / totalNumber || 0);
        const numNewConts = cids.length;
        if (numNewConts === 0) return { assigned: 0, newContNumbers: new Set(), logs: [], statusInserts: [] };
        let remainingQtyToAssign = detailQty;
        let remainingWeightToAssign = assignedDetailWeight;
        let sumThis = 0;
        let newContNumbers = new Set();
        let pendingLogs = [];
        let pendingStatusInserts = [];
        for (let i = 0; i < numNewConts; i++) {
          const cid = cids[i];
          // Refetch availability to handle race
          const availQuery = `
            SELECT 
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
            LEFT JOIN container_hire_details chd ON cm.cid = chd.cid
            LEFT JOIN (
              SELECT DISTINCT ON (cid) cid, location, availability
              FROM container_status
              ORDER BY cid, sid DESC NULLS LAST
            ) cs ON cm.cid = cs.cid
            WHERE cm.cid = $1
          `;
          const availResult = await client.query(availQuery, [cid]);
          if (availResult.rows[0]?.derived_status && !['Available', 'Assigned to Job'].includes(availResult.rows[0].derived_status)) {
            console.warn(`Container ${cid} no longer eligible during assignment`);
            continue;
          }
          const fullCont = fullContainerMap.get(cid);
          if (!fullCont) continue;
          const contNum = fullCont.container_number;
          newContNumbers.add(contNum);
          const isLast = i === numNewConts - 1;
          const thisQty = isLast ? remainingQtyToAssign : Math.floor(detailQty / numNewConts);
          const thisWeight = isLast ? remainingWeightToAssign : assignedDetailWeight * (Math.floor(detailQty / numNewConts) / detailQty);
          remainingQtyToAssign -= thisQty;
          remainingWeightToAssign -= thisWeight;
          const priorLocation = priorLocations.get(cid) || 'karachi_port';
          const prevStatusResult = await client.query(`
            SELECT availability as status
            FROM container_status
            WHERE cid = $1
            ORDER BY created_time DESC
            LIMIT 1
          `, [cid]);
          const previousStatus = prevStatusResult.rows[0]?.status || 'Available';
          let enhancedStatusNotes = `Assigned ${thisQty} items from ${priorLocation.toUpperCase()} to Order ${orderId} / Receiver ${recId} / Detail ${detailId} / Cont ${contNum}`;
          let enhancedLogNotes = `Assigned ${thisQty} items to container ${contNum} for Order ${orderId} / Receiver ${recId} / Detail ${detailId} (remaining: ${remainingItems}) from ${priorLocation.toUpperCase()}`;
          // Optional: Log for reassignments
          if (fullCont.derived_status === 'Assigned to Job') {
            console.warn(`Reassigning already 'Assigned to Job' container ${contNum} (CID ${cid}) to new detail ${detailId}`);
            enhancedLogNotes += ' (Reassignment from prior job)';
          }
          pendingLogs.push([
            cid,
            contNum,
            orderId,
            recId,
            detailId,
            thisQty,
            'Assigned to Job',
            'ASSIGN',
            created_by,
            enhancedLogNotes,
            previousStatus
          ]);
          pendingStatusInserts.push([cid, priorLocation, enhancedStatusNotes, created_by]);
          sumThis += thisQty;
        }
        // Insert
        for (let k = 0; k < pendingLogs.length; k++) {
          const logValues = pendingLogs[k];
          const tempNotes = logValues[9];
          const fullNotes = `${tempNotes} (ETA: ${receiverEta || 'N/A'})`;
          logValues[9] = fullNotes;
          await client.query(logQuery, logValues);
        }
        for (let k = 0; k < pendingStatusInserts.length; k++) {
          const [cid, location, tempStatusNotes, created_by_val] = pendingStatusInserts[k];
          const fullStatusNotes = `${tempStatusNotes} (ETA: ${receiverEta || 'N/A'})`;
          await client.query(statusInsertQuery, [cid, 'Assigned to Job', location, fullStatusNotes, created_by_val]);
        }
        return { assigned: sumThis, newContNumbers, logs: pendingLogs, statusInserts: pendingStatusInserts };
      } catch (err) {
        console.error(`Error in assignToDetail for detail ${detailId}:`, err);
        return { assigned: 0, newContNumbers: new Set(), logs: [], statusInserts: [] };
      }
    }

    // Helper to get available containers - for auto multi
    async function getAvailableContainers() {
      const availQuery = `
        SELECT 
          cm.*,
          cs.location as status_location,
          cs.availability as current_availability,
          chd.hire_start_date,
          chd.hire_end_date,
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
        LEFT JOIN container_hire_details chd ON cm.cid = chd.cid
        LEFT JOIN (
          SELECT DISTINCT ON (cid) cid, location, availability
          FROM container_status
          ORDER BY cid, sid DESC NULLS LAST
        ) cs ON cm.cid = cs.cid
        WHERE (
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
        ) IN ('Available', 'Assigned to Job') AND cs.location IN ('karachi_port', 'dubai_port')
        ORDER BY cm.cid
      `;
      const availResult = await client.query(availQuery);
      return availResult.rows;
    }

    for (const orderId of orderIds) {
      const orderAssignments = resolvedAssignments[orderId] || {};
      const currentOrder = currentOrders[orderId];
      if (!currentOrder) continue;
      let currentTotalAssigned = parseInt(currentOrder.total_assigned_qty) || 0;
      let assignedForThisOrder = 0;
      let assignedGrossWeight = 0;
      let assignedCount = 0;
      const orderReceivers = [];
      let orderEta = currentOrder.eta || new Date().toISOString().split('T')[0];

      if (singleMode) {
        // Single container assignment to all remaining details across all receivers in the order
        console.log(`Single container assigning to all remaining details in order ${orderId} using container ${singleContNum}`);
        // Check availability once before starting
        const availQuerySingle = `
          SELECT 
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
          LEFT JOIN container_hire_details chd ON cm.cid = chd.cid
          LEFT JOIN (
            SELECT DISTINCT ON (cid) cid, location, availability
            FROM container_status
            ORDER BY cid, sid DESC NULLS LAST
          ) cs ON cm.cid = cs.cid
          WHERE cm.cid = $1
        `;
        const availResult = await client.query(availQuerySingle, [singleCid]);
        if (availResult.rows[0]?.derived_status && !['Available', 'Assigned to Job'].includes(availResult.rows[0].derived_status)) {
          console.warn(`Container ${singleCid} not eligible for order ${orderId}`);
          continue;
        }
        const receiversQuerySingle = `
          SELECT id, receiver_name, containers, qty_delivered, eta, etd, status, total_weight, total_number 
          FROM receivers 
          WHERE order_id = $1 AND total_number > COALESCE(qty_delivered, 0)
        `;
        const receiversResult = await client.query(receiversQuerySingle, [orderId]);
        let allPendingLogs = [];
        let allAssignedDetailIds = [];
        let totalAssignedToCont = 0;
        let receiverUpdates = []; // Collect receiver update queries
        for (const recRow of receiversResult.rows) {
          const recId = recRow.id;
          const receiver = recRow;
          orderReceivers.push(receiver);
          const remainingDetailsQuery = `
            SELECT oi.id as detail_id, oi.total_number, oi.weight, COALESCE(SUM(h.assigned_qty), 0) as current_assigned
            FROM order_items oi
            LEFT JOIN container_assignment_history h ON oi.id = h.detail_id
            WHERE oi.receiver_id = $1
            GROUP BY oi.id, oi.total_number, oi.weight
            HAVING oi.total_number > COALESCE(SUM(h.assigned_qty), 0)
            ORDER BY oi.id
          `;
          const remainingDetailsResult = await client.query(remainingDetailsQuery, [recId]);
          let sumQty = 0;
          let sumGross = 0;
          let receiverEta = receiver.eta || orderEta;
          let recAssignedDetailIds = [];
          let recPendingLogs = [];
          for (const detailRow of remainingDetailsResult.rows) {
            const detailId = detailRow.detail_id;
            const totalNumber = parseInt(detailRow.total_number);
            const detailWeight = parseFloat(detailRow.weight) || 0;
            const currentAssigned = parseInt(detailRow.current_assigned) || 0;
            const remaining = totalNumber - currentAssigned;
            if (remaining <= 0) continue;
            const thisQty = remaining;
            const assignedDetailWeight = detailWeight * (thisQty / totalNumber);
            let enhancedLogNotes = `Assigned ${thisQty} items to container ${singleContNum} for Order ${orderId} / Receiver ${recId} / Detail ${detailId}`;
            // Optional: Log for reassignments
            if (singleFullCont.derived_status === 'Assigned to Job') {
              console.warn(`Reassigning already 'Assigned to Job' container ${singleContNum} (CID ${singleCid}) to new detail ${detailId}`);
              enhancedLogNotes += ' (Reassignment from prior job)';
            }
            recPendingLogs.push([
              singleCid,
              singleContNum,
              orderId,
              recId,
              detailId,
              thisQty,
              'Assigned to Job',
              'ASSIGN',
              created_by,
              enhancedLogNotes,
              singlePreviousStatus
            ]);
            totalAssignedToCont += thisQty;
            allAssignedDetailIds.push(detailId);
            recAssignedDetailIds.push(detailId);
            sumQty += thisQty;
            sumGross += assignedDetailWeight;
          }
          if (recPendingLogs.length > 0) {
            // Prepare receiver update
            let currentContainers = [];
            if (receiver.containers && typeof receiver.containers === 'string') {
              try {
                currentContainers = JSON.parse(receiver.containers);
              } catch (e) {
                currentContainers = [];
              }
            }
            const allContainers = new Set([...currentContainers, singleContNum]);
            const updatedContainersJson = JSON.stringify(Array.from(allContainers));
            let newEta = receiverEta;
            let newEtd = receiver.etd || new Date().toISOString().split('T')[0];
            const newDelivered = (parseInt(receiver.qty_delivered) || 0) + sumQty;
            const receiverSet = [];
            const receiverValues = [];
            let receiverParamIndex = 1;
            const receiverFields = [
              { key: 'containers', val: updatedContainersJson },
              { key: 'qty_delivered', val: newDelivered },
              { key: 'eta', val: newEta || null },
              { key: 'etd', val: newEtd || null },
              { key: 'status', val: 'Ready for Loading' }
            ];
            receiverFields.forEach(field => {
              receiverSet.push(`${field.key} = $${receiverParamIndex}`);
              receiverValues.push(field.val);
              receiverParamIndex++;
            });
            receiverSet.push('updated_at = CURRENT_TIMESTAMP');
            receiverValues.push(recId);
            const updateReceiverQuery = `
              UPDATE receivers 
              SET ${receiverSet.join(', ')} 
              WHERE id = $${receiverParamIndex}
              RETURNING id, status, eta
            `;
            receiverUpdates.push({ query: updateReceiverQuery, values: receiverValues, sumQty, sumGross, receiver, recId });
            allPendingLogs = allPendingLogs.concat(recPendingLogs);
          }
        }
        // Now, after collecting all, insert logs and status once
        if (allPendingLogs.length > 0) {
          // Refetch eligibility final check
          const finalAvailResult = await client.query(availQuerySingle, [singleCid]);
          if (finalAvailResult.rows[0]?.derived_status && !['Available', 'Assigned to Job'].includes(finalAvailResult.rows[0].derived_status)) {
            console.warn(`Container ${singleCid} no longer eligible at final insert for order ${orderId}`);
            continue;
          }
          // Insert all history logs
          for (const logValues of allPendingLogs) {
            const tempNotes = logValues[9];
            const fullNotes = `${tempNotes} (ETA: ${orderEta || 'N/A'})`;
            logValues[9] = fullNotes;
            await client.query(logQuery, logValues);
          }
          // Insert one status for the whole order
          const assignedReceiversList = receiverUpdates.map(ru => ru.recId).join(', ');
          const enhancedStatusNotes = `Assigned ${totalAssignedToCont} items from ${singlePriorLocation.toUpperCase()} to Order ${orderId} / Receivers ${assignedReceiversList} / Cont ${singleContNum} (details: ${allAssignedDetailIds.join(', ')})`;
          const fullStatusNotes = `${enhancedStatusNotes} (ETA: ${orderEta || 'N/A'})`;
          await client.query(statusInsertQuery, [singleCid, 'Assigned to Job', singlePriorLocation, fullStatusNotes, created_by]);
          // Now execute receiver updates
          for (const ru of receiverUpdates) {
            const updateResult = await client.query(ru.query, ru.values);
            if (updateResult.rowCount > 0) {
              assignedCount++;
              assignedForThisOrder += ru.sumQty;
              assignedGrossWeight += ru.sumGross;
              trackingData.push({
                receiverId: ru.recId,
                receiverName: ru.receiver.receiver_name,
                orderId,
                bookingRef: currentOrder.booking_ref,
                assignedQty: ru.sumQty,
                assignedGross: ru.sumGross.toFixed(2),
                assignedContainers: [singleContNum],
                status: currentOrder.overall_status,
                newEta: ru.receiver.eta,
                newEtd: ru.receiver.etd || new Date().toISOString().split('T')[0],
                daysUntilEta: null
              });
              console.log(`Single container assigned ${ru.sumQty} to receiver ${ru.recId}`);
            }
          }
        }
      } else if (Object.keys(orderAssignments).length > 0) {
        // Specific assignments - original code
        for (const recIdStr of Object.keys(orderAssignments)) {
          const recId = parseInt(recIdStr);
          if (isNaN(recId)) continue;
          const receiverQuery = `
            SELECT id, receiver_name, containers, qty_delivered, eta, etd, status, total_weight, total_number 
            FROM receivers 
            WHERE id = $1 AND order_id = $2
          `;
          const receiverResult = await client.query(receiverQuery, [recId, orderId]);
          if (receiverResult.rowCount === 0) continue;
          const receiver = receiverResult.rows[0];
          orderReceivers.push(receiver);
          let currentContainers = [];
          if (receiver.containers && typeof receiver.containers === 'string') {
            try {
              currentContainers = JSON.parse(receiver.containers);
            } catch (e) {
              currentContainers = [];
            }
          }
          const itemsQuery = `
            SELECT id, total_number, weight
            FROM order_items 
            WHERE receiver_id = $1 
            ORDER BY id ASC
          `;
          const itemsResult = await client.query(itemsQuery, [recId]);
          const orderItemsIds = itemsResult.rows.map(row => row.id);
          const orderItemsMap = new Map(itemsResult.rows.map(row => [row.id, row]));
          const recAssignments = orderAssignments[recIdStr];
          const sortedDetailKeys = Object.keys(recAssignments).sort((a, b) => parseInt(a) - parseInt(b));
          let sumQty = 0;
          let sumGross = 0;
          let newContNumbers = new Set();
          for (const detailIdxStr of sortedDetailKeys) {
            const detailIndex = parseInt(detailIdxStr);
            if (isNaN(detailIndex) || detailIndex >= orderItemsIds.length) continue;
            let intendedDetailId = orderItemsIds[detailIndex];
            const detailAssign = recAssignments[detailIdxStr];
            const detailCids = detailAssign.containers || [];
            if (detailCids.length === 0) continue;
            const resolvedCidsThis = detailCids;
            if (resolvedCidsThis.length === 0) continue;
            const result = await assignToDetail(orderId, recId, intendedDetailId, parseInt(detailAssign.qty) || 0, resolvedCidsThis, fullContainerMap, priorLocations, receiver.eta);
            if (result.assigned > 0) {
              sumQty += result.assigned;
              result.newContNumbers.forEach(num => newContNumbers.add(num));
              sumGross += (receiver.total_weight || 0) * (result.assigned / (receiver.total_number || 1));  // Approximate gross
            }
          }
          if (sumQty > 0) {
            const allContainers = new Set([...currentContainers, ...newContNumbers]);
            const updatedContainersJson = JSON.stringify(Array.from(allContainers));
            let newEta = receiver.eta;
            let newEtd = receiver.etd || new Date().toISOString().split('T')[0];
            const newDelivered = (parseInt(receiver.qty_delivered) || 0) + sumQty;
            const receiverSet = [];
            const receiverValues = [];
            let receiverParamIndex = 1;
            const receiverFields = [
              { key: 'containers', val: updatedContainersJson },
              { key: 'qty_delivered', val: newDelivered },
              { key: 'eta', val: newEta || null },
              { key: 'etd', val: newEtd || null },
              { key: 'status', val: 'Ready for Loading' }
            ];
            receiverFields.forEach(field => {
              receiverSet.push(`${field.key} = $${receiverParamIndex}`);
              receiverValues.push(field.val);
              receiverParamIndex++;
            });
            receiverSet.push('updated_at = CURRENT_TIMESTAMP');
            receiverValues.push(recId);
            const updateReceiverQuery = `
              UPDATE receivers 
              SET ${receiverSet.join(', ')} 
              WHERE id = $${receiverParamIndex}
              RETURNING id, status, eta
            `;
            const updateResult = await client.query(updateReceiverQuery, receiverValues);
            if (updateResult.rowCount > 0) {
              assignedCount++;
              assignedForThisOrder += sumQty;
              assignedGrossWeight += sumGross;
              trackingData.push({
                receiverId: recId,
                receiverName: receiver.receiver_name,
                orderId,
                bookingRef: currentOrder.booking_ref,
                assignedQty: sumQty,
                assignedGross: sumGross.toFixed(2),
                assignedContainers: Array.from(newContNumbers),
                status: currentOrder.overall_status,
                newEta,
                newEtd,
                daysUntilEta: null
              });
              console.log(`Specific assigned ${sumQty} to receiver ${recId}`);
            }
          }
        }
      } else {
        // Original auto-assignment for orders without specific assignments
        console.log(`Auto-assigning to remaining details in order ${orderId}`);
        const receiversResult = await client.query('SELECT id FROM receivers WHERE order_id = $1 AND (total_number > COALESCE(qty_delivered, 0))', [orderId]);
        let availableContainers = await getAvailableContainers();
        let contIndex = 0;
        for (const recRow of receiversResult.rows) {
          const recId = recRow.id;
          const receiverQuery = `
            SELECT id, receiver_name, containers, qty_delivered, eta, etd, status, total_weight, total_number 
            FROM receivers 
            WHERE id = $1
          `;
          const receiverResult = await client.query(receiverQuery, [recId]);
          if (receiverResult.rowCount === 0) continue;
          const receiver = receiverResult.rows[0];
          orderReceivers.push(receiver);
          const remainingDetailsQuery = `
            SELECT oi.id, oi.total_number, oi.weight, COALESCE(SUM(h.assigned_qty), 0) as assigned
            FROM order_items oi
            LEFT JOIN container_assignment_history h ON oi.id = h.detail_id
            WHERE oi.receiver_id = $1
            GROUP BY oi.id, oi.total_number, oi.weight
            HAVING oi.total_number > COALESCE(SUM(h.assigned_qty), 0)
            ORDER BY oi.id
          `;
          const remainingDetailsResult = await client.query(remainingDetailsQuery, [recId]);
          let sumQty = 0;
          let sumGross = 0;
          let newContNumbers = new Set();
          for (const detailRow of remainingDetailsResult.rows) {
            const detailId = detailRow.id;
            const remaining = parseInt(detailRow.total_number) - parseInt(detailRow.assigned);
            if (contIndex >= availableContainers.length) {
              availableContainers = await getAvailableContainers();
              contIndex = 0;
            }
            if (availableContainers.length === 0) break;
            const fullCont = availableContainers[contIndex];
            const cids = [fullCont.cid];
            const result = await assignToDetail(orderId, recId, detailId, remaining, cids, new Map(availableContainers.map(c => [c.cid, c])), priorLocations, receiver.eta);
            if (result.assigned > 0) {
              sumQty += result.assigned;
              result.newContNumbers.forEach(num => newContNumbers.add(num));
              sumGross += parseFloat(detailRow.weight) * (result.assigned / parseInt(detailRow.total_number) || 0);
            }
            contIndex++;
          }
          if (sumQty > 0) {
            let currentContainers = [];
            if (receiver.containers && typeof receiver.containers === 'string') {
              try {
                currentContainers = JSON.parse(receiver.containers);
              } catch (e) {
                currentContainers = [];
              }
            }
            const allContainers = new Set([...currentContainers, ...newContNumbers]);
            const updatedContainersJson = JSON.stringify(Array.from(allContainers));
            let newEta = receiver.eta;
            let newEtd = receiver.etd || new Date().toISOString().split('T')[0];
            const newDelivered = (parseInt(receiver.qty_delivered) || 0) + sumQty;
            const receiverSet = [];
            const receiverValues = [];
            let receiverParamIndex = 1;
            const receiverFields = [
              { key: 'containers', val: updatedContainersJson },
              { key: 'qty_delivered', val: newDelivered },
              { key: 'eta', val: newEta || null },
              { key: 'etd', val: newEtd || null },
              { key: 'status', val: 'Ready for Loading' }
            ];
            receiverFields.forEach(field => {
              receiverSet.push(`${field.key} = $${receiverParamIndex}`);
              receiverValues.push(field.val);
              receiverParamIndex++;
            });
            receiverSet.push('updated_at = CURRENT_TIMESTAMP');
            receiverValues.push(recId);
            const updateReceiverQuery = `
              UPDATE receivers 
              SET ${receiverSet.join(', ')} 
              WHERE id = $${receiverParamIndex}
              RETURNING id, status, eta
            `;
            const updateResult = await client.query(updateReceiverQuery, receiverValues);
            if (updateResult.rowCount > 0) {
              assignedCount++;
              assignedForThisOrder += sumQty;
              assignedGrossWeight += sumGross;
              trackingData.push({
                receiverId: recId,
                receiverName: receiver.receiver_name,
                orderId,
                bookingRef: currentOrder.booking_ref,
                assignedQty: sumQty,
                assignedGross: sumGross.toFixed(2),
                assignedContainers: Array.from(newContNumbers),
                status: currentOrder.overall_status,
                newEta,
                newEtd,
                daysUntilEta: null
              });
              console.log(`Auto assigned ${sumQty} to receiver ${recId}`);
            }
          }
        }
      }

      // Update order if assigned something
      if (assignedForThisOrder > 0) {
        const newTotalAssigned = currentTotalAssigned + assignedForThisOrder;
        const orderSet = [];
        const orderValues = [];
        let orderParamIndex = 1;
        const orderFields = [
          { key: 'total_assigned_qty', val: newTotalAssigned },
          { key: 'eta', val: orderEta }
        ];
        orderFields.forEach(field => {
          orderSet.push(`${field.key} = $${orderParamIndex}`);
          orderValues.push(field.val);
          orderParamIndex++;
        });
        orderSet.push('updated_at = CURRENT_TIMESTAMP');
        orderValues.push(orderId);
        const updateOrderQuery = `
          UPDATE orders 
          SET ${orderSet.join(', ')} 
          WHERE id = $${orderParamIndex}
          RETURNING id, total_assigned_qty, eta
        `;
        const orderUpdateResult = await client.query(updateOrderQuery, orderValues);
        if (orderUpdateResult.rowCount > 0) {
          console.log(`Updated total_assigned_qty to ${newTotalAssigned} for order ${orderId}`);
        }
        await updateOrderOverallStatus(client, orderId, 'Loaded Into Container', orderReceivers);
      }
      updatedOrders.push({ 
        orderId: currentOrder.id, 
        bookingRef: currentOrder.booking_ref, 
        assignedReceivers: assignedCount,
        assignedQty: assignedForThisOrder,
        assignedGross: assignedGrossWeight.toFixed(2)
      });
    }
    await client.query('COMMIT');

    // Refetch updated data (same as before, with containerDetails from history)
    const enhancedUpdatedOrders = [];
    const safeIntCast = (val) => `COALESCE(${val}, 0)`;
    const safeNumericCast = (val) => `COALESCE(${val}, 0)`;
    const totalAssignedSub = `(SELECT COALESCE(SUM(assigned_qty), 0) FROM container_assignment_history WHERE detail_id = oi.id)`;
    const containerDetailsSub = `
      COALESCE((
        SELECT json_agg(
          json_build_object(
            'status', CASE WHEN ${totalAssignedSub} > 0 THEN 'Ready for Loading' ELSE 'Created' END,
            'container', json_build_object(
              'cid', h.cid,
              'container_number', cm.container_number
            ),
            'total_number', ${safeIntCast('oi.total_number')},
            'assign_weight', CASE 
              WHEN ${totalAssignedSub} > 0 AND h.assigned_qty > 0 THEN 
                ROUND((h.assigned_qty::numeric / ${totalAssignedSub}::numeric * ${safeNumericCast('oi.weight')} / 1000), 2)::text 
              ELSE '0' 
            END,
            'remaining_items', (${safeIntCast('oi.total_number')} - ${totalAssignedSub})::text,
            'assign_total_box', h.assigned_qty::text
          ) ORDER BY cm.container_number
        )
        FROM (
          SELECT cid, SUM(assigned_qty) as assigned_qty 
          FROM container_assignment_history 
          WHERE detail_id = oi.id 
          GROUP BY cid
        ) h 
        LEFT JOIN container_master cm ON h.cid = cm.cid 
        WHERE h.cid IS NOT NULL
      ), '[]'::json)
    `;
    const fetchReceiversQuery = `
      SELECT 
        r.*,
        COALESCE(
          (
            SELECT json_agg(
              json_build_object(
                'pickupLocation', oi.pickup_location,
                'deliveryAddress', oi.delivery_address,
                'category', oi.category,
                'subcategory', oi.subcategory,
                'type', oi.type,
                'totalNumber', ${safeIntCast('oi.total_number')},
                'weight', ${safeNumericCast('oi.weight')},
                'itemRef', oi.item_ref,
                'containerDetails', ${containerDetailsSub},
                'remainingItems', ${safeIntCast('oi.total_number')} - ${totalAssignedSub}
              ) ORDER BY oi.id
            )
            FROM order_items oi 
            WHERE oi.receiver_id = r.id
          ), 
          '[]'::json
        ) AS shippingDetails
      FROM receivers r 
      WHERE r.order_id = $1 
      ORDER BY r.id
    `;
    for (const updOrder of updatedOrders) {
      const orderId = updOrder.orderId;
      const updatedOrderResult = await client.query('SELECT * FROM orders WHERE id = $1', [orderId]);
      const updatedSenderResult = await client.query('SELECT * FROM senders WHERE order_id = $1', [orderId]);
      const enhancedReceiversResult = await client.query(fetchReceiversQuery, [orderId]);
      const enhancedReceivers = enhancedReceiversResult.rows.map(row => ({
        ...row,
        shippingDetails: row.shippingDetails || [],
        containers: typeof row.containers === 'string' ? JSON.parse(row.containers) : (row.containers || [])
      }));
      let orderSummary = [];  
      try {
        const summaryResult = await client.query('SELECT * FROM order_summary WHERE order_id = $1', [orderId]);
        orderSummary = summaryResult.rows;
      } catch (e) {
        console.warn('order_summary fetch failed:', e.message);
        orderSummary = [];  // Fallback as before if needed
      }
      // Fetch container_assignment_history details for this order
      const historyQuery = `
        SELECT 
          h.*,
          cm.container_number
        FROM container_assignment_history h
        LEFT JOIN container_master cm ON h.cid = cm.cid
        WHERE h.order_id = $1
        ORDER BY h.id DESC
      `;
      const historyResult = await client.query(historyQuery, [orderId]);
      enhancedUpdatedOrders.push({
        order: updatedOrderResult.rows[0],
        senders: updatedSenderResult.rows,
        summary: orderSummary,
        receivers: enhancedReceivers,
        assignmentHistory: historyResult.rows,  // Added full history details
        tracking: trackingData.filter(t => t.orderId === orderId)
      });
    }
    const responseData = { 
      success: true, 
      message: `Assigned containers to ${trackingData.length} receivers across ${updatedOrders.length} orders`,
      updatedOrders: enhancedUpdatedOrders,
      tracking: trackingData 
    };
    if (skippedDetails.length > 0) {
      responseData.skippedDetails = skippedDetails;
      console.warn(`Skipped ${skippedDetails.length} over-assigned details:`, skippedDetails);
    }
    res.status(200).json(responseData);
  } catch (error) {
    console.error('Error assigning containers:', error);
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackErr) {
        console.error('Rollback failed:', rollbackErr);
      }
    }
    // Error codes as before...
    return res.status(500).json({ error: 'Internal server error', details: error.message });
  } finally {
    if (client) client.release();
  }
}
export async function assignContainersToOrders(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    const updates = req.body || {};
    const created_by = req.user?.id || 'system';  // From auth if available

    // Helper: Sanitize numeric fields for JSON (prevents "" in strings)
    function sanitizeForJson(val, isNumeric = true) {
      if (isNumeric) {
        const num = parseFloat(val) || parseInt(val, 10) || 0;
        return num.toString();
      }
      return val || '';
    }

    // NEW: Bulk sanitize entire container_details array (cleans existing entries with "")
    function bulkSanitizeContainerDetails(details) {
      if (!Array.isArray(details)) return details || [];
      return details.map(cd => {
        if (typeof cd !== 'object' || !cd) return cd;  // Skip invalid
        const numericFields = ['total_number', 'assign_weight', 'remaining_items', 'assign_total_box'];
        numericFields.forEach(field => {
          if (field in cd) {
            cd[field] = sanitizeForJson(cd[field]);
          }
        });
        return cd;
      });
    }

    // Extract assignments (support wrapped or direct)
    let assignments = updates.assignments || updates;
    if (!assignments || typeof assignments !== 'object') {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'Assignments object is required' });
    }

    // Infer orderIds from keys of assignments (ensure array)
    let orderIds = Object.keys(assignments).filter(k => !isNaN(parseInt(k)));
    if (orderIds.length === 0) {
      // Fallback: If explicit order_ids provided, use it
      orderIds = updates.order_ids || updates.orderIds || [];
      if (Array.isArray(orderIds)) {
        orderIds = orderIds.filter(id => !isNaN(parseInt(id)));
      } else {
        orderIds = [];
      }
    }

    // Debug log (enhanced)
    console.log('Assign containers body:', JSON.stringify(updates, null, 2));
    console.log('Extracted assignments keys (orderIds):', orderIds);
    console.log('Sample assignment:', assignments[orderIds[0]] ? Object.keys(assignments[orderIds[0]]).slice(0, 2) : null);

    // Validation
    const updateErrors = [];
    if (orderIds.length === 0) {
      updateErrors.push('orderIds array is required and must not be empty');
    }
    if (Object.keys(assignments).length === 0) {
      updateErrors.push('assignments object is required and must not be empty');
    }
    if (updateErrors.length > 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({
        error: 'Invalid request fields',
        details: updateErrors.join('; ')
      });
    }

    // Collect all unique container IDs from the assignments (enhanced logging)
    const allCids = new Set();
    let parsedCount = 0;
    for (const [orderIdStr, orderAssign] of Object.entries(assignments)) {
      if (typeof orderAssign !== 'object') continue;
      console.log(`Parsing order ${orderIdStr}: ${Object.keys(orderAssign).length} receivers`);
      for (const [recIdStr, recAssign] of Object.entries(orderAssign)) {
        if (typeof recAssign !== 'object' || recAssign === null) continue;
        console.log(`  Parsing receiver ${recIdStr}: ${Object.keys(recAssign).length} details`);
        for (const [detailIdxStr, detailAssign] of Object.entries(recAssign)) {
          if (detailAssign && typeof detailAssign === 'object') {
            console.log(`    Detail ${detailIdxStr}: qty=${detailAssign.qty}, containers=${JSON.stringify(detailAssign.containers)}`);
            if (Array.isArray(detailAssign.containers)) {
              detailAssign.containers.forEach((cidStr, idx) => {
                const cid = parseInt(cidStr);
                if (!isNaN(cid)) {
                  allCids.add(cid);
                  parsedCount++;
                  console.log(`      Valid CID ${cid} from "${cidStr}"`);
                } else {
                  console.warn(`      Invalid CID "${cidStr}" (NaN)`);
                }
              });
            } else {
              console.warn(`    No valid containers array in detail ${detailIdxStr}`);
            }
          }
        }
      }
    }
    console.log(`Total valid CIDs extracted: ${allCids.size} (parsed ${parsedCount} items)`);

    if (allCids.size === 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'No valid containers specified in assignments' });
    }

    // Batched: Fetch prior physical locations for all CIDs (default to 'karachi_port')
    const priorQuery = `
      SELECT DISTINCT ON (cid) cid, location
      FROM container_status 
      WHERE cid = ANY($1) ORDER BY cid, created_time DESC
    `;
    const priorResult = await client.query(priorQuery, [Array.from(allCids)]);
    const priorLocations = new Map(priorResult.rows.map(row => [row.cid, row.location || 'karachi_port']));

    // Fetch full container details for all unique cids - ensure only available OR assigned to job
    const containerQuery = `
      SELECT 
        cm.*,
        cs.location as status_location,
        cs.availability as current_availability,
        chd.hire_start_date,
        chd.hire_end_date,
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
      ) IN ('Available', 'Assigned to Job') AND cs.location IN ('karachi_port', 'dubai_port')
    `;
    const containerResult = await client.query(containerQuery, [Array.from(allCids)]);
    console.log('Container query for CIDs', Array.from(allCids), 'returned', containerResult.rows.length, 'rows');
    containerResult.rows.forEach(row => {
      console.log('  - CID', row.cid, ':', row.container_number, '(status:', row.derived_status, ')');
    });
    const fullContainerMap = new Map(containerResult.rows.map(row => [row.cid, row]));
    const containerNumberMap = new Map(containerResult.rows.map(row => [row.cid, row.container_number]));
    // Check for missing or unavailable containers
    const missingCids = Array.from(allCids).filter(cid => !fullContainerMap.has(cid));
    console.log('Missing/unavailable CIDs:', missingCids);
    if (missingCids.length > 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: `Containers not found, not available, or not in eligible status: ${missingCids.join(', ')}` });
    }

    // Track updates
    const updatedOrders = [];
    const trackingData = [];
    // Accumulate total assigned qty per order for final update
    const orderAssignedQtys = new Map();
    const allReceiversForOrders = [];  // Collect for overall status update
    // Fetch current orders and related records for fallback
    const currentOrders = {};
    const currentReceiversByOrder = {};
    for (const orderIdStr of orderIds) {
      const orderId = parseInt(orderIdStr);
      if (isNaN(orderId)) {
        console.warn(`Invalid order_id: ${orderIdStr}`);
        continue;
      }
      // Fetch current order
      const orderQuery = `
        SELECT id, booking_ref, status as overall_status, total_assigned_qty, eta 
        FROM orders 
        WHERE id = $1
      `;
      const orderResult = await client.query(orderQuery, [orderId]);
      if (orderResult.rowCount === 0) {
        console.warn(`Order not found: ${orderId}`);
        continue;
      }
      currentOrders[orderId] = orderResult.rows[0];
      // Fetch current receivers for this order
      const receiversQuery = `
        SELECT id, receiver_name, containers, qty_delivered, eta, etd, status, total_weight, total_number 
        FROM receivers 
        WHERE order_id = $1
      `;
      const receiversResult = await client.query(receiversQuery, [orderId]);
      currentReceiversByOrder[orderId] = receiversResult.rows;
    }

    const logQuery = `
      INSERT INTO container_assignment_history (
        cid, container_number, order_id, receiver_id, detail_id, assigned_qty, status, action_type, changed_by, notes, previous_status
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    `;

    for (const orderIdStr of orderIds) {
      const orderId = parseInt(orderIdStr);
      if (isNaN(orderId)) {
        console.warn(`Invalid order_id: ${orderIdStr}`);
        continue;
      }
      const currentOrder = currentOrders[orderId];
      if (!currentOrder) {
        console.warn(`No current order data for: ${orderId}`);
        continue;
      }
      if (!assignments[orderIdStr]) {
        console.warn(`No assignments for order: ${orderId}`);
        updatedOrders.push({ orderId, assigned: 0 });
        continue;
      }
      let currentTotalAssigned = parseInt(currentOrder.total_assigned_qty) || 0;
      let assignedForThisOrder = 0; // Accumulate for this order
      let assignedGrossWeight = 0;  // Accumulate gross
      let assignedCount = 0;
      const orderAssignments = assignments[orderIdStr];
      const orderReceivers = [];  // Collect for status update
      for (const recIdStr of Object.keys(orderAssignments)) {
        const recId = parseInt(recIdStr);
        if (isNaN(recId)) {
          console.warn(`Invalid receiver_id: ${recIdStr}`);
          continue;
        }
        // Fetch receiver (include eta, etd, status for calculation; add total_weight for gross)
        const receiverQuery = `
          SELECT id, receiver_name, containers, qty_delivered, eta, etd, status, total_weight, total_number 
          FROM receivers 
          WHERE id = $1 AND order_id = $2
        `;
        const receiverResult = await client.query(receiverQuery, [recId, orderId]);
        if (receiverResult.rowCount === 0) {
          console.warn(`Receiver not found: ${recId} for order ${orderId}`);
          continue;
        }
        const receiver = receiverResult.rows[0];
        orderReceivers.push(receiver);  // For status
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
        // Fetch order_items for this receiver
        const itemsQuery = `
          SELECT id, total_number, weight, container_details 
          FROM order_items 
          WHERE receiver_id = $1 
          ORDER BY id ASC
        `;
        const itemsResult = await client.query(itemsQuery, [recId]);
        const orderItemRows = itemsResult.rows;
        if (orderItemRows.length === 0) {
          console.warn(`No order_items found for receiver ${recId}`);
          continue;
        }
        const orderItemsIds = orderItemRows.map(row => row.id);
        const orderItemsMap = new Map(orderItemRows.map(row => [row.id, row]));
        // Collect new containers and sum qty/gross for this receiver
        const newContNumbers = new Set();
        let sumQty = 0;
        let sumGross = 0;
        const recAssignments = orderAssignments[recIdStr];
        // Sort detail keys numerically to process in order
        const sortedDetailKeys = Object.keys(recAssignments).sort((a, b) => parseInt(a) - parseInt(b));
        let pendingLogs = [];
        let pendingStatusInserts = [];
        for (const detailIdxStr of sortedDetailKeys) {
          const detailIndex = parseInt(detailIdxStr);
          if (isNaN(detailIndex) || detailIndex < 0 || detailIndex >= orderItemsIds.length) {
            console.warn(`Invalid detail index ${detailIdxStr} for receiver ${recId} (only ${orderItemsIds.length} items)`);
            continue;
          }
          const actualDetailId = orderItemsIds[detailIndex];
          const orderItem = orderItemsMap.get(actualDetailId);
          if (!orderItem) continue;
          const totalNumber = parseInt(orderItem.total_number) || 0;
          const detailWeight = parseFloat(orderItem.weight) || 0;
          const inputDetailQty = parseInt(recAssignments[detailIdxStr].qty) || 0;
          if (inputDetailQty === 0) continue;
          // Fetch current assigned from history
          const currentAssignedQuery = `
            SELECT COALESCE(SUM(assigned_qty), 0) as current_assigned
            FROM container_assignment_history 
            WHERE detail_id = $1
          `;
          const currentAssignedResult = await client.query(currentAssignedQuery, [actualDetailId]);
          const currentAssigned = parseInt(currentAssignedResult.rows[0].current_assigned) || 0;
          const remainingBefore = totalNumber - currentAssigned;
          let detailQty = inputDetailQty;
          if (detailQty > remainingBefore) {
            console.warn(`Capping assignment for detail ${actualDetailId}: ${detailQty} > ${remainingBefore}`);
            detailQty = remainingBefore;
            if (detailQty <= 0) continue;
          }
          const totalAssigned = currentAssigned + detailQty;
          const remainingItems = totalNumber - totalAssigned;
          const assignedDetailWeight = detailWeight * (detailQty / totalNumber || 0);
          let currentContainerDetails = [];
          if (orderItem.container_details) {
            if (typeof orderItem.container_details === 'string') {
              try {
                currentContainerDetails = JSON.parse(orderItem.container_details);
              } catch (e) {
                console.warn(`Failed to parse container_details for detail ${actualDetailId}`);
              }
            } else if (Array.isArray(orderItem.container_details)) {
              currentContainerDetails = orderItem.container_details;
            }
          }
          // NEW: Bulk sanitize loaded details to clean existing "" entries
          currentContainerDetails = bulkSanitizeContainerDetails(currentContainerDetails);
          const detailAssign = recAssignments[detailIdxStr];
          const detailCids = [];
          if (Array.isArray(detailAssign.containers)) {
            detailAssign.containers.forEach(cidStr => {
              const cid = parseInt(cidStr);
              if (!isNaN(cid) && fullContainerMap.has(cid)) {
                detailCids.push(cid);
              }
            });
          }
          const numNewConts = detailCids.length;
          if (numNewConts === 0) continue;
          let remainingQtyToAssign = detailQty;
          let remainingWeightToAssign = assignedDetailWeight;
          for (let i = 0; i < numNewConts; i++) {
            const cid = detailCids[i];
            const priorLocation = priorLocations.get(cid) || 'karachi_port';  // Fallback
            const fullCont = fullContainerMap.get(cid);
            if (!fullCont) continue;
            const contNum = fullCont.container_number;
            newContNumbers.add(contNum);
            const isLast = i === numNewConts - 1;
            const thisQty = isLast ? remainingQtyToAssign : Math.floor(detailQty / numNewConts);
            const thisWeight = isLast ? remainingWeightToAssign : assignedDetailWeight * (Math.floor(detailQty / numNewConts) / detailQty);
            remainingQtyToAssign -= thisQty;
            remainingWeightToAssign -= thisWeight;
            // Check for existing entry
            let existingIndex = -1;
            for (let j = 0; j < currentContainerDetails.length; j++) {
              if (currentContainerDetails[j].container?.cid === cid) {
                existingIndex = j;
                break;
              }
            }
            const newEntry = {
              status: "Ready for Loading",
              container: {
                cid: cid,
                container_number: contNum
              },
              total_number: sanitizeForJson(totalNumber),  // FIXED: Was ""
              assign_weight: sanitizeForJson(Math.round(thisWeight / 1000)),
              remaining_items: sanitizeForJson(remainingItems),
              assign_total_box: sanitizeForJson(thisQty)
            };
            if (existingIndex !== -1) {
              const existing = currentContainerDetails[existingIndex];
              const newBox = parseInt(sanitizeForJson(existing.assign_total_box)) + thisQty;
              const newW = parseFloat(sanitizeForJson(existing.assign_weight)) + Math.round(thisWeight / 1000);
              currentContainerDetails[existingIndex] = {
                ...existing,
                total_number: sanitizeForJson(totalNumber),
                assign_total_box: sanitizeForJson(newBox),
                assign_weight: sanitizeForJson(newW),
                remaining_items: sanitizeForJson(remainingItems)
              };
            } else {
              currentContainerDetails.push(newEntry);
            }
            // Fetch previous status
            const prevStatusQuery = `
              SELECT availability as status
              FROM container_status
              WHERE cid = $1
              ORDER BY created_time DESC
              LIMIT 1
            `;
            const prevStatusResult = await client.query(prevStatusQuery, [cid]);
            const previousStatus = prevStatusResult.rows[0]?.status || 'Available';
            // Enhanced notes with physical location
            let enhancedStatusNotes = `Assigned ${thisQty} items from ${priorLocation.toUpperCase()} to Order ${orderId} / Receiver ${recId} / Detail ${actualDetailId} / Cont ${contNum}`;
            let enhancedLogNotes = `Assigned ${thisQty} items to container ${contNum} for Order ${orderId} / Receiver ${recId} / Detail ${actualDetailId} (remaining: ${remainingItems}) from ${priorLocation.toUpperCase()}`;
            // Optional: Log for reassignments
            if (fullCont.derived_status === 'Assigned to Job') {
              console.warn(`Reassigning already 'Assigned to Job' container ${contNum} (CID ${cid}) to new detail ${actualDetailId}`);
              enhancedLogNotes += ' (Reassignment from prior job)';
            }
            // Collect for log
            pendingLogs.push([
              cid, contNum, orderId, recId, actualDetailId, thisQty, 'Assigned to Job',
              'ASSIGN', created_by, enhancedLogNotes, previousStatus
            ]);
            // Collect for status insert (location stays physical)
            pendingStatusInserts.push([cid, priorLocation, enhancedStatusNotes, created_by]);
          }
          // Update all remaining_items in currentContainerDetails
          currentContainerDetails.forEach(cd => {
            cd.remaining_items = sanitizeForJson(remainingItems);
          });
          // Update order_item container_details
          const updateItemQuery = `
            UPDATE order_items 
            SET container_details = $1, updated_at = CURRENT_TIMESTAMP
            WHERE id = $2
          `;
          await client.query(updateItemQuery, [JSON.stringify(currentContainerDetails), actualDetailId]);
          // Accumulate
          sumQty += detailQty;
          sumGross += assignedDetailWeight;
        }
        if (sumQty === 0 && newContNumbers.size === 0) {
          console.warn(`No qty or containers to assign for receiver ${recId}`);
          continue;
        }
        // Append unique new containers
        const allContainers = new Set([...currentContainers, ...newContNumbers]);
        const updatedContainersJson = JSON.stringify(Array.from(allContainers));
        // Since assigned, now calculate eta/etd
        let newEta = receiver.eta;
        let newEtd = receiver.etd;
        let daysUntilEta = null;
        const nowHasContainers = allContainers.size > 0;
        if (nowHasContainers && !newEta) {
          const etaResult = await calculateETA(client, receiver.status || 'Created');
          newEta = etaResult.eta;
          daysUntilEta = etaResult.daysUntil;
          console.log(`[assignContainers] Calculated new ETA for receiver ${recId}: ${newEta} (status: ${receiver.status}, daysUntil: ${daysUntilEta})`);
        } else if (newEta) {
          daysUntilEta = computeDaysUntilEta(newEta);
        }
        // ETD: Set to today if not set
        if (nowHasContainers && !newEtd) {
          newEtd = new Date().toISOString().split('T')[0];
          console.log(`[assignContainers] Set ETD for receiver ${recId} to today: ${newEtd}`);
        }
        // Insert pending logs and status updates with ETA
        for (let k = 0; k < pendingLogs.length; k++) {
          const logValues = pendingLogs[k];
          const tempNotes = logValues[9];
          const fullNotes = `${tempNotes} (ETA: ${newEta || 'N/A'})`;
          logValues[9] = fullNotes;
          await client.query(logQuery, logValues);
        }
        const statusInsertQuery = `
          INSERT INTO container_status (cid, availability, location, status_notes, created_by, created_time) 
          VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
        `;
        for (let k = 0; k < pendingStatusInserts.length; k++) {
          const [cid, location, tempStatusNotes, created_by_val] = pendingStatusInserts[k];
          const fullStatusNotes = `${tempStatusNotes} (ETA: ${newEta || 'N/A'})`;
          await client.query(statusInsertQuery, [cid, 'Assigned to Job', location, fullStatusNotes, created_by_val]);
        }
        // Update qty_delivered
        const newDelivered = (parseInt(receiver.qty_delivered) || 0) + sumQty;
        // Update receiver (dynamic SET) - removed total_weight update; added status
        const receiverSet = [];
        const receiverValues = [];
        let receiverParamIndex = 1;
        const receiverFields = [
          { key: 'containers', val: updatedContainersJson },
          { key: 'qty_delivered', val: newDelivered },
          { key: 'eta', val: newEta || null },
          { key: 'etd', val: newEtd || null },
          { key: 'status', val: 'Ready for Loading' }
        ];
        receiverFields.forEach(field => {
          receiverSet.push(`${field.key} = $${receiverParamIndex}`);
          receiverValues.push(field.val);
          receiverParamIndex++;
        });
        receiverSet.push('updated_at = CURRENT_TIMESTAMP');
        receiverValues.push(recId);
        const updateReceiverQuery = `
          UPDATE receivers 
          SET ${receiverSet.join(', ')} 
          WHERE id = $${receiverParamIndex}
          RETURNING id, status, eta
        `;
        const updateResult = await client.query(updateReceiverQuery, receiverValues);
        if (updateResult.rowCount > 0) {
          assignedCount++;
          assignedForThisOrder += sumQty;
          assignedGrossWeight += sumGross;
          const updatedRec = updateResult.rows[0];
          trackingData.push({
            receiverId: recId,
            receiverName: receiver.receiver_name,
            orderId: currentOrder.id,
            bookingRef: currentOrder.booking_ref,
            assignedQty: sumQty,
            assignedGross: sumGross.toFixed(2),
            assignedContainers: Array.from(newContNumbers),
            status: currentOrder.overall_status,
            newEta: newEta,
            newEtd: newEtd,
            daysUntilEta
          });
          console.log(`Assigned ${sumQty} qty, ${sumGross.toFixed(2)} gross, ${newContNumbers.size} containers to receiver ${recId} (order ${orderId}); ETA/ETD: ${newEta}/${newEtd} (days: ${daysUntilEta})`);
        } else {
          console.warn(`No rows updated for receiver ${recId}`);
        }
      }
      // After processing all receivers for this order, update the order
      if (assignedForThisOrder > 0) {
        const newTotalAssigned = currentTotalAssigned + assignedForThisOrder;
        orderAssignedQtys.set(orderId, newTotalAssigned);
        const orderSet = [];
        const orderValues = [];
        let orderParamIndex = 1;
        const orderFields = [
          { key: 'total_assigned_qty', val: newTotalAssigned },
          { key: 'eta', val: currentOrder.eta || new Date().toISOString().split('T')[0] }
        ];
        orderFields.forEach(field => {
          orderSet.push(`${field.key} = $${orderParamIndex}`);
          orderValues.push(field.val);
          orderParamIndex++;
        });
        orderSet.push('updated_at = CURRENT_TIMESTAMP');
        orderValues.push(orderId);
        const updateOrderQuery = `
          UPDATE orders 
          SET ${orderSet.join(', ')} 
          WHERE id = $${orderParamIndex}
          RETURNING id, total_assigned_qty, eta
        `;
        const orderUpdateResult = await client.query(updateOrderQuery, orderValues);
        if (orderUpdateResult.rowCount === 0) {
          console.warn(`Failed to update total_assigned_qty/eta for order ${orderId}`);
        } else {
          console.log(`Updated total_assigned_qty to ${newTotalAssigned} and ETA for order ${orderId}`);
        }
        // Cascade overall status
        await updateOrderOverallStatus(client, orderId, 'Loaded Into Container', orderReceivers);
      }
      allReceiversForOrders.push(...orderReceivers);
      updatedOrders.push({ 
        orderId: currentOrder.id, 
        bookingRef: currentOrder.booking_ref, 
        assignedReceivers: assignedCount,
        assignedQty: assignedForThisOrder,
        assignedGross: assignedGrossWeight.toFixed(2)
      });
    }
    await client.query('COMMIT');
    // Refetch updated data - for all affected orders (with safe casting)
    const enhancedUpdatedOrders = [];
    const safeIntCast = (val) => `COALESCE(${val}, 0)`;
    const safeNumericCast = (val) => `COALESCE(${val}, 0)`;
    const totalAssignedSub = `(SELECT COALESCE(SUM(assigned_qty), 0) FROM container_assignment_history WHERE detail_id = oi.id)`;
    const containerDetailsSub = `
      COALESCE((
        SELECT json_agg(
          json_build_object(
            'status', CASE WHEN ${totalAssignedSub} > 0 THEN 'Ready for Loading' ELSE 'Created' END,
            'container', json_build_object(
              'cid', h.cid,
              'container_number', cm.container_number
            ),
            'total_number', ${safeIntCast('oi.total_number')},
            'assign_weight', CASE 
              WHEN ${totalAssignedSub} > 0 AND h.assigned_qty > 0 THEN 
                ROUND((h.assigned_qty::numeric / ${totalAssignedSub}::numeric * ${safeNumericCast('oi.weight')} / 1000), 2)::text 
              ELSE '0' 
            END,
            'remaining_items', (${safeIntCast('oi.total_number')} - ${totalAssignedSub})::text,
            'assign_total_box', h.assigned_qty::text
          ) ORDER BY cm.container_number
        )
        FROM (
          SELECT cid, SUM(assigned_qty) as assigned_qty 
          FROM container_assignment_history 
          WHERE detail_id = oi.id 
          GROUP BY cid
        ) h 
        LEFT JOIN container_master cm ON h.cid = cm.cid 
        WHERE h.cid IS NOT NULL
      ), '[]'::json)
    `;
    const safeIntFromJson = (jsonPath) => `COALESCE(CASE WHEN (${jsonPath}) ~ '^[0-9]+$' THEN (${jsonPath})::int ELSE 0 END, 0)`;
    for (const updOrder of updatedOrders) {
      const orderId = updOrder.orderId;
      // Refetch order
      const updatedOrderResult = await client.query('SELECT * FROM orders WHERE id = $1', [orderId]);
      // Fetch senders
      const updatedSenderResult = await client.query('SELECT * FROM senders WHERE order_id = $1', [orderId]);
      // Fetch enhanced receivers with shippingDetails (safe casting) - using history for containerDetails
      const fetchReceiversQuery = `
        SELECT 
          r.*,
          COALESCE(
            (
              SELECT json_agg(
                json_build_object(
                  'pickupLocation', oi.pickup_location,
                  'deliveryAddress', oi.delivery_address,
                  'category', oi.category,
                  'subcategory', oi.subcategory,
                  'type', oi.type,
                  'totalNumber', ${safeIntCast('oi.total_number')} - COALESCE((SELECT SUM(${safeIntFromJson('elem->>\'assign_total_box\'')}) FROM jsonb_array_elements(COALESCE(oi.container_details, '[]'::jsonb)) AS elem), 0),
                  'weight', oi.weight,
                  'itemRef', oi.item_ref,
                  'containerDetails', ${containerDetailsSub},
                  'remainingItems', ${safeIntCast('oi.total_number')} - ${totalAssignedSub}
                ) ORDER BY oi.id
              )
              FROM order_items oi 
              WHERE oi.receiver_id = r.id
            ), 
            '[]'::json
          ) AS shippingDetails
        FROM receivers r 
        WHERE r.order_id = $1 
        ORDER BY r.id
      `;
      const enhancedReceiversResult = await client.query(fetchReceiversQuery, [orderId]);
      const enhancedReceivers = enhancedReceiversResult.rows.map(row => ({
        ...row,
        shippingDetails: row.shippingDetails || [],
        containers: typeof row.containers === 'string' ? JSON.parse(row.containers) : (row.containers || [])
      }));
      // Fetch summary
      let orderSummary = [];
      try {
        const summaryQuery = 'SELECT * FROM order_summary WHERE order_id = $1';
        const summaryResult = await client.query(summaryQuery, [orderId]);
        orderSummary = summaryResult.rows;
      } catch (summaryErr) {
        console.warn('order_summary view fetch failed:', summaryErr.message);
        const fallbackQuery = `
          SELECT o.id as order_id, o.booking_ref, o.status, o.created_at, o.eta, o.etd, o.shipping_line,
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
          LEFT JOIN order_tracking ot ON o.id = ot.order_id
          LEFT JOIN container_master cm ON ot.container_id = cm.cid
          LEFT JOIN LATERAL (
            SELECT 
              json_agg(json_build_object(
                'id', r.id,
                'receiver_name', r.receiver_name,
                'total_number', r.total_number,
                'status', r.status
              )) AS receiver_summary
            FROM receivers r
            WHERE r.order_id = o.id
          ) rs ON true
          LEFT JOIN LATERAL (
            SELECT 
              json_object_agg(status, count(*)::text) AS receiver_status_summary
            FROM receivers r
            WHERE r.order_id = o.id
            GROUP BY status
          ) rss ON true
          LEFT JOIN LATERAL (
            SELECT 
              json_agg(DISTINCT elem) AS receiver_containers_json
            FROM receivers re,
                 LATERAL json_array_elements_text(re.containers::json) AS elem
            WHERE re.order_id = o.id AND re.containers IS NOT NULL AND re.containers != '[]'
          ) rc ON true
          LEFT JOIN LATERAL (
            SELECT 
              COALESCE(SUM(r.total_number), 0) AS total_items,
              COALESCE(SUM(r.total_number - COALESCE(r.qty_delivered, 0)), 0) AS remaining_items
            FROM receivers r
            WHERE r.order_id = o.id
          ) rt ON true
          LEFT JOIN LATERAL (
            SELECT json_agg(json_build_object(
              'id', r.id,
              'receiver_name', r.receiver_name,
              'shippingDetails', COALESCE((
                SELECT json_agg(
                  json_build_object(
                    'pickupLocation', oi.pickup_location,
                    'deliveryAddress', oi.delivery_address,
                    'category', oi.category,
                    'subcategory', oi.subcategory,
                    'type', oi.type,
                    'totalNumber', ${safeIntCast('oi.total_number')} - COALESCE((SELECT SUM(${safeIntFromJson('elem->>\'assign_total_box\'')}) FROM jsonb_array_elements(COALESCE(oi.container_details, '[]'::jsonb)) AS elem), 0),
                    'weight', oi.weight,
                    'itemRef', oi.item_ref,
                    'containerDetails', ${containerDetailsSub},
                    'remainingItems', ${safeIntCast('oi.total_number')} - ${totalAssignedSub}
                  ) ORDER BY oi.id
                )
                FROM order_items oi 
                WHERE oi.receiver_id = r.id
              ), '[]'::json)
            ) ORDER BY r.id) AS receivers_details
            FROM receivers r
            WHERE r.order_id = o.id
          ) rd ON true
          WHERE o.id = $1
        `;
        const fallbackResult = await client.query(fallbackQuery, [orderId]);
        orderSummary = fallbackResult.rows;
      }
      // Fetch container_assignment_history details for this order
      const historyQuery = `
        SELECT 
          h.*,
          cm.container_number
        FROM container_assignment_history h
        LEFT JOIN container_master cm ON h.cid = cm.cid
        WHERE h.order_id = $1
        ORDER BY h.id DESC
      `;
      const historyResult = await client.query(historyQuery, [orderId]);
      enhancedUpdatedOrders.push({
        order: updatedOrderResult.rows[0],
        senders: updatedSenderResult.rows,
        summary: orderSummary,
        receivers: enhancedReceivers,
        assignmentHistory: historyResult.rows,
        tracking: trackingData.filter(t => t.orderId === orderId)
      });
    }
    res.status(200).json({ 
      success: true, 
      message: `Assigned containers to ${trackingData.length} receivers across ${updatedOrders.length} orders`,
      updatedOrders: enhancedUpdatedOrders,
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
    if (error.code === '23505') {
      return res.status(409).json({ error: 'Duplicate entry conflict' });
    }
    if (error.code === '23514') {
      return res.status(400).json({ error: 'Invalid value for constrained field' });
    }
    if (error.code === '22007' || error.code === '22008') {
      return res.status(400).json({ error: 'Invalid date format' });
    }
    if (error.code === '22023') {  // JSON errors
      return res.status(400).json({ error: 'Invalid JSON in container details' });
    }
    if (error.code === '22P02') {  // Casting errors
      return res.status(500).json({ error: 'Invalid data format in order details. Please check numeric fields.' });
    }
    if (error.code === '42P01' || error.code === '42703' || error.code === '42P10') {
      return res.status(500).json({ error: 'Database schema or query scoping mismatch. Check view definitions and run migrations.' });
    }
    return res.status(500).json({ error: 'Internal server error', details: error.message });
  } finally {
    if (client) {
      client.release();
    }
  }
}
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
export async function getOrderByItemRef(req, res) {
  const { ref } = req.params;

  if (!ref || typeof ref !== 'string' || ref.trim() === '') {
    return res.status(400).json({
      success: false,
      message: 'Item reference is required'
    });
  }

  const pattern = `%${ref.trim().toUpperCase()}%`;

  console.log(`[trackByItemRef] Searching for item ref pattern: "${pattern}"`);

  try {
    const query = `
      SELECT 
        o.id AS order_id,
        o.booking_ref,
        o.created_at,
        o.status AS order_base_status,
        o.eta,
        o.etd,
        o.place_of_loading,          -- added
        o.place_of_delivery,         -- added
        o.total_assigned_qty,
        s.sender_name,
        s.sender_contact,
        s.sender_email,
        t.transport_type,
        t.drop_method,
        t.delivery_date,
        r.id AS receiver_id,
        r.receiver_name,
        r.receiver_contact,
        r.receiver_email,
        r.receiver_address,
        r.status AS receiver_base_status,
        r.eta AS receiver_eta,
        r.containers AS receiver_containers,
        COALESCE((
          SELECT json_agg(
            json_build_object(
              'drop_method', d.drop_method,
              'dropoff_name', d.dropoff_name,
              'drop_off_cnic', d.drop_off_cnic,
              'drop_off_mobile', d.drop_off_mobile,
              'plate_no', d.plate_no,
              'drop_date', d.drop_date
            ) ORDER BY d.id
          )
          FROM drop_off_details d
          WHERE d.order_id = o.id 
            AND (d.receiver_id = r.id OR d.receiver_id IS NULL)
        ), '[]'::json) AS drop_off_details,
        oi.id AS item_id,
        oi.item_ref,
        oi.category,
        oi.subcategory,
        oi.type,
        oi.total_number,
        oi.weight,
        cah.id AS assignment_id,
        cm.container_number,
        cah.assigned_qty,
        cah.status AS assign_status,
        cah.created_at AS assign_created_at,
        cah.notes AS assign_notes,
        ct.id AS ct_tracking_id,
        ct.new_status AS ct_new_status,
        ct.timestamp AS ct_timestamp,
        ct.details AS ct_details,
        ct.event_type AS ct_event_type
      FROM order_items oi
      INNER JOIN orders o ON oi.order_id = o.id
      LEFT JOIN senders s ON s.order_id = o.id
      LEFT JOIN transport_details t ON t.order_id = o.id
      LEFT JOIN receivers r ON oi.receiver_id = r.id
      LEFT JOIN container_assignment_history cah ON cah.detail_id = oi.id
      LEFT JOIN container_master cm ON cm.cid = cah.cid
      LEFT JOIN consignments c ON (
        c.orders @> jsonb_build_array(o.id::text) OR
        c.orders @> jsonb_build_array(o.id)
      )
      LEFT JOIN consignment_tracking ct ON ct.consignment_id = c.id
        AND ct.event_type IN ('status_advanced', 'status_updated', 'status_auto_updated')
      WHERE oi.item_ref ILIKE $1
      ORDER BY o.created_at DESC, oi.id, cah.created_at, ct.timestamp DESC
    `;

    const { rows } = await pool.query(query, [pattern]);

    if (rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: `No items found matching reference "${ref.trim()}"`
      });
    }

    // ────────────────────────────────────────────────
    // Group into clean nested structure
    // ────────────────────────────────────────────────
    const orderMap = {};

    rows.forEach(row => {
      const orderId = row.order_id;

      if (!orderMap[orderId]) {
        orderMap[orderId] = {
          order_id: orderId,
          booking_ref: row.booking_ref,
          created_at: row.created_at,
          status: row.order_base_status,
          eta: row.eta,
          etd: row.etd,
          place_of_loading: row.place_of_loading || null,     // added here
          place_of_delivery: row.place_of_delivery || null,   // added here
          total_assigned_qty: row.total_assigned_qty || 0,
          sender: {
            name: row.sender_name || '—',
            contact: row.sender_contact || '—',
            email: row.sender_email || '—'
          },
          transport: {
            type: row.transport_type || '—',
            drop_method: row.drop_method || '—',
            delivery_date: row.delivery_date || '—'
          },
          receivers: {}
        };
      }

      const ord = orderMap[orderId];

      const recvKey = row.receiver_id ? row.receiver_id : `no_receiver_${orderId}`;

      if (!ord.receivers[recvKey]) {
        const statusSequence = [
          'Order Created',
          'Ready for Loading',
          'Loaded Into Container',
          'Shipment Processing',
          'Shipment In Transit',
          'Under Processing',
          'Arrived at Sort Facility',
          'Ready for Delivery',
          'Shipment Delivered'
        ];

        const receiverCurrent = row.receiver_base_status || 'Order Created';
        const currentIdx = statusSequence.indexOf(receiverCurrent);
        const remaining = currentIdx === -1 || currentIdx >= statusSequence.length - 1
          ? []
          : statusSequence.slice(currentIdx + 1);

        ord.receivers[recvKey] = {
          receiver_id: row.receiver_id || null,
          name: row.receiver_name || 'Unassigned',
          contact: row.receiver_contact || '—',
          email: row.receiver_email || '—',
          address: row.receiver_address || '—',
          status: receiverCurrent,
          eta: row.receiver_eta || null,
          containers: row.receiver_containers || [],
          drop_off_details: row.drop_off_details || [],
          items: {},
          current_status: receiverCurrent,
          status_history: [],
          remaining_status_steps: remaining
        };
      }

      const recv = ord.receivers[recvKey];

      // Add consignment tracking entries
      if (row.ct_tracking_id && !recv.status_history.some(h => h.tracking_id === row.ct_tracking_id)) {
        recv.status_history.push({
          tracking_id: row.ct_tracking_id,
          status: row.ct_new_status,
          time: row.ct_timestamp,
          event_type: row.ct_event_type,
          details: row.ct_details || {},
          notes: (row.ct_details?.notes || '') + 
                (row.ct_details?.reason ? ` (${row.ct_details.reason})` : '') +
                (row.ct_details?.location ? ` at ${row.ct_details.location}` : '')
        });
      }

      if (row.item_id) {
        if (!recv.items[row.item_id]) {
          const total = Number(row.total_number) || 0;
          const assigned = rows
            .filter(r => r.item_id === row.item_id)
            .reduce((sum, r) => sum + (Number(r.assigned_qty) || 0), 0);

          recv.items[row.item_id] = {
            item_id: row.item_id,
            item_ref: row.item_ref || '—',
            category: row.category || '—',
            subcategory: row.subcategory || '—',
            type: row.type || '—',
            total_number: total,
            weight: Number(row.weight) || 0,
            assigned_qty: assigned,
            remaining_items: total - assigned,
            progress_percent: total > 0 ? Math.round((assigned / total) * 100) : 0,
            assignments: []
          };
        }

        if (row.assignment_id) {
          recv.items[row.item_id].assignments.push({
            assignment_id: row.assignment_id,
            container_number: row.container_number || '—',
            assigned_qty: Number(row.assigned_qty) || 0,
            status: row.assign_status || '—',
            created_at: row.assign_created_at,
            notes: row.assign_notes || ''
          });
        }
      }
    });

    // Finalize response
    const result = Object.values(orderMap).map(order => {
      Object.values(order.receivers).forEach(recv => {
        recv.status_history.sort((a, b) => new Date(b.time) - new Date(a.time));

        if (recv.status_history.length > 0) {
          recv.current_status = recv.status_history[0].status;
        }
      });

      return {
        ...order,
        receivers: Object.values(order.receivers).map(r => ({
          ...r,
          items: Object.values(r.items)
        }))
      };
    });

    res.json({
      success: true,
      data: result.length === 1 ? result[0] : result,
      count: result.length,
      message: `Found ${result.length} order(s) containing item reference matching "${ref.trim()}"`
    });

  } catch (err) {
    console.error('getOrderByItemRef error:', err.stack || err);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch item tracking details',
      error: process.env.NODE_ENV === 'development' ? err.message : undefined
    });
  }
}
export async function getOrderByTrackingId(req, res) {
 const { id } = req.params;  // ← change 'number' to the real name (e.g. 'id', 'consignment', etc.)

  if (!id?.trim()) {
    return res.status(400).json({ success: false, message: 'Consignment number required' });
  }

  const consNumber = id.trim().toUpperCase();
  try {
    // 1. Fetch consignment core data
    const consRes = await pool.query(`
      SELECT 
        c.id AS consignment_id,
        c.consignment_number,
        c.status AS consignment_status,
        c.eta AS consignment_eta,
        c.origin,
        c.destination,
        c.shipping_line,
        c.vessel,
        c.voyage,
        c.seal_no,
        c.net_weight,
        c.gross_weight,
        c.consignment_value,
        c.currency_code,
        c.containers
      FROM consignments c
      WHERE c.consignment_number = $1
    `, [consNumber]);

    if (consRes.rowCount === 0) {
      return res.status(404).json({ success: false, message: 'Consignment not found' });
    }

    const cons = consRes.rows[0];

    // Parse containers safely
    let containers = [];
    try {
      containers = typeof cons.containers === 'string' ? JSON.parse(cons.containers) : cons.containers || [];
    } catch (e) {
      console.warn('Invalid consignment containers JSON:', e);
    }

    // 2. Find linked order IDs
    const orderIdsRes = await pool.query(`
      SELECT jsonb_array_elements_text(c.orders)::int AS order_id
      FROM consignments c
      WHERE c.id = $1
    `, [cons.consignment_id]);

    const orderIds = orderIdsRes.rows.map(r => r.order_id).filter(id => id > 0);

    let orders = [];

    if (orderIds.length > 0) {
      const ordersRes = await pool.query(`
        SELECT 
          o.id AS order_id,
          o.booking_ref,
          o.created_at,
          o.status,
          o.eta,
          o.etd,
          t.collection_scope,
          o.total_assigned_qty,
          s.sender_name,
          s.sender_contact,
          s.sender_email,
          t.transport_type,
          t.drop_method,
          t.delivery_date,
          r.id AS receiver_id,
          r.receiver_name,
          r.receiver_contact,
          r.receiver_email,
          r.receiver_address,
          r.status AS receiver_status,
          r.eta AS receiver_eta,
          r.containers AS receiver_containers,
          -- Aggregate drop_off_details per receiver
          COALESCE((
            SELECT json_agg(
              json_build_object(
                'drop_method', d.drop_method,
                'dropoff_name', d.dropoff_name,
                'drop_off_cnic', d.drop_off_cnic,
                'drop_off_mobile', d.drop_off_mobile,
                'plate_no', d.plate_no,
                'drop_date', d.drop_date
              ) ORDER BY d.id
            )
            FROM drop_off_details d
            WHERE d.order_id = o.id 
              AND (d.receiver_id = r.id OR d.receiver_id IS NULL)
          ), '[]'::json) AS drop_off_details,
          oi.id AS item_id,
          oi.item_ref,
          oi.category,
          oi.subcategory,
          oi.type,
          oi.total_number,
          oi.weight,
          cah.id AS assignment_id,
          cm.container_number,
          cah.assigned_qty,
          cah.status AS assign_status,
          cah.created_at AS assign_created_at,
          cah.notes AS assign_notes
        FROM orders o
        LEFT JOIN senders s ON s.order_id = o.id
        LEFT JOIN transport_details t ON t.order_id = o.id
        LEFT JOIN receivers r ON r.order_id = o.id
        LEFT JOIN order_items oi ON oi.order_id = o.id AND oi.receiver_id = r.id
        LEFT JOIN container_assignment_history cah ON cah.detail_id = oi.id
        LEFT JOIN container_master cm ON cm.cid = cah.cid
        WHERE o.id = ANY($1)
        ORDER BY o.id, r.id, oi.id, cah.created_at
      `, [orderIds]);

      // ────────────────────────────────────────────────
      // Group into nested structure
      // ────────────────────────────────────────────────
      const orderMap = {};

      ordersRes.rows.forEach(row => {
        const orderId = row.order_id;

        if (!orderMap[orderId]) {
          orderMap[orderId] = {
            order_id: orderId,
            booking_ref: row.booking_ref,
            created_at: row.created_at,
            status: row.status,
            overall_status: row.status || 'Created', // temporary fallback
            eta: row.eta,
            etd: row.etd,
            collection_scope: row.collection_scope || '—',
            total_assigned_qty: row.total_assigned_qty || 0,
            sender: {
              name: row.sender_name || '—',
              contact: row.sender_contact || '—',
              email: row.sender_email || '—'
            },
            transport: {
              type: row.transport_type || '—',
              drop_method: row.drop_method || '—',
              delivery_date: row.delivery_date || '—'
            },
            receivers: {},
            summary: {
              total_items: 0,
              total_weight: 0,
              total_assigned: 0,
              active_containers: new Set()
            }
          };
        }

        const ord = orderMap[orderId];

        // Update summary
        if (row.total_number) {
          ord.summary.total_items += Number(row.total_number);
          ord.summary.total_weight += Number(row.weight || 0);
        }
        if (row.container_number) {
          ord.summary.active_containers.add(row.container_number);
        }
        ord.summary.total_assigned += Number(row.assigned_qty || 0);

        // Receiver grouping
        const recvKey = row.receiver_id ? row.receiver_id : `no_receiver_${orderId}`;
        if (!ord.receivers[recvKey]) {
          ord.receivers[recvKey] = {
            receiver_id: row.receiver_id || null,
            name: row.receiver_name || 'Unassigned',
            contact: row.receiver_contact || '—',
            email: row.receiver_email || '—',
            address: row.receiver_address || '—',
            status: row.receiver_status || '—',
            eta: row.receiver_eta || null,
            containers: row.receiver_containers || [],
            drop_off_details: row.drop_off_details || [],
            items: {}
          };
        }

        const recv = ord.receivers[recvKey];

        if (row.item_id) {
          if (!recv.items[row.item_id]) {
            recv.items[row.item_id] = {
              item_id: row.item_id,
              item_ref: row.item_ref || '—',
              category: row.category || '—',
              subcategory: row.subcategory || '—',
              type: row.type || '—',
              total_number: Number(row.total_number) || 0,
              weight: Number(row.weight) || 0,
              assignments: []
            };
          }

          if (row.assignment_id) {
            recv.items[row.item_id].assignments.push({
              assignment_id: row.assignment_id,
              container_number: row.container_number || '—',
              assigned_qty: Number(row.assigned_qty) || 0,
              status: row.assign_status || '—',
              created_at: row.assign_created_at,
              notes: row.assign_notes || ''
            });
          }
        }
      });

      // Finalize each order
      const result = Object.values(orderMap).map(order => {
        // Compute real progress
        let totalItems = 0;
        let totalAssigned = 0;
        order.receivers = Object.values(order.receivers).map(r => {
          Object.values(r.items).forEach(item => {
            totalItems += item.total_number;
            totalAssigned += item.assignments.reduce((s, a) => s + a.assigned_qty, 0);
          });
          return r;
        });

        order.summary = {
          ...order.summary,
          total_items: totalItems,
          total_assigned: totalAssigned,
          progress_percent: totalItems > 0 ? Math.round((totalAssigned / totalItems) * 100) : 0,
          active_containers: Array.from(order.summary.active_containers)
        };

        // Smarter overall_status from receivers
        const receiverStatuses = Object.values(order.receivers).map(r => r.status || '');
        const statusOrder = ['Delivered', 'Shipment In Transit', 'In Transit', 'Under Shipment Processing', 'Loaded Into Container', 'Ready for Loading', 'In Process', 'Created'];
        const highest = receiverStatuses.reduce((best, curr) => {
          const pri = statusOrder.indexOf(curr);
          const bestPri = statusOrder.indexOf(best);
          return pri > bestPri ? curr : best;
        }, order.status || 'Created');

        order.overall_status = highest;

        return {
          ...order,
          receivers: Object.values(order.receivers).map(r => ({
            ...r,
            items: Object.values(r.items)
          }))
        };
      });

      res.json({
        success: true,
        data: {
          consignment: {
            id: cons.consignment_id,
            number: cons.consignment_number,
            status: cons.consignment_status,
            eta: cons.consignment_eta,
            origin: cons.origin,
            destination: cons.destination,
            shipping_line: cons.shipping_line,
            vessel: cons.vessel,
            voyage: cons.voyage,
            seal_no: cons.seal_no,
            net_weight: cons.net_weight,
            gross_weight: cons.gross_weight,
            value: cons.consignment_value,
            currency: cons.currency_code,
            containers
          },
          orders: result,
          summary: {
            order_count: result.length,
            total_assigned: result.reduce((s, o) => s + o.total_assigned_qty, 0),
            total_items: result.reduce((s, o) => s + (o.summary?.total_items || 0), 0),
            progress_percent: result.length > 0 
              ? Math.round(result.reduce((s, o) => s + (o.summary?.progress_percent || 0), 0) / result.length)
              : 0,
            active_containers: [...new Set(result.flatMap(o => o.summary?.active_containers || []))],
            latest_activity: result.reduce((m, o) => o.created_at > m ? o.created_at : m, '1970-01-01')
          }
        }
      });

  }
 } catch (err) {
    console.error('getOrderByTrackingId error:', err.stack || err);
    res.status(500).json({
      success: false,
      message: 'Server error while tracking consignment',
      details: process.env.NODE_ENV === 'development' ? err.message : undefined
    });
  }
}

export async function getOrderByOrderId(req, res) {
  const { ref } = req.params;

  if (!ref || typeof ref !== 'string' || ref.trim() === '') {
    return res.status(400).json({
      success: false,
      message: 'Order reference (ID or booking ref) is required'
    });
  }

  const search = ref.trim();
  const isNumeric = !isNaN(Number(search)) && Number.isInteger(Number(search));

  try {
    const query = `
      SELECT 
        o.id AS order_id,
        o.booking_ref,
        o.created_at,
        o.status,
        o.eta,
        o.etd,
        t.collection_scope,
        o.total_assigned_qty,
        s.sender_name,
        s.sender_contact,
        s.sender_email,
        t.transport_type,
        t.drop_method,
        t.delivery_date,
        r.id AS receiver_id,
        r.receiver_name,
        r.receiver_contact,
        r.receiver_email,
        r.receiver_address,
        r.status AS receiver_status,
        r.eta AS receiver_eta,
        r.containers AS receiver_containers,
        -- Aggregate drop_off_details per receiver
        COALESCE((
          SELECT json_agg(
            json_build_object(
              'drop_method', d.drop_method,
              'dropoff_name', d.dropoff_name,
              'drop_off_cnic', d.drop_off_cnic,
              'drop_off_mobile', d.drop_off_mobile,
              'plate_no', d.plate_no,
              'drop_date', d.drop_date
            ) ORDER BY d.id
          )
          FROM drop_off_details d
          WHERE d.order_id = o.id 
            AND (d.receiver_id = r.id OR d.receiver_id IS NULL)
        ), '[]'::json) AS drop_off_details,
        oi.id AS item_id,
        oi.item_ref,
        oi.category,
        oi.subcategory,
        oi.type,
        oi.total_number,
        oi.weight,
        cah.id AS assignment_id,
        cm.container_number,
        cah.assigned_qty,
        cah.status AS assign_status,
        cah.created_at AS assign_created_at,
        cah.notes AS assign_notes
      FROM orders o
      LEFT JOIN senders s ON s.order_id = o.id
      LEFT JOIN transport_details t ON t.order_id = o.id
      LEFT JOIN receivers r ON r.order_id = o.id
      LEFT JOIN order_items oi ON oi.order_id = o.id AND oi.receiver_id = r.id
      LEFT JOIN container_assignment_history cah ON cah.detail_id = oi.id
      LEFT JOIN container_master cm ON cm.cid = cah.cid
      WHERE ${isNumeric ? 'o.id = $1' : 'o.booking_ref ILIKE $1'}
         OR o.booking_ref ILIKE $2
      ORDER BY o.id, r.id, oi.id, cah.created_at
    `;

    const params = isNumeric 
      ? [Number(search), `%${search}%`] 
      : [`%${search}%`, `%${search}%`];

    const { rows } = await pool.query(query, params);

    if (rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: `No order found for reference "${search}"`
      });
    }

    // ────────────────────────────────────────────────
    // Group into nested structure
    // ────────────────────────────────────────────────
    const orderMap = {};

    rows.forEach(row => {
      const orderId = row.order_id;

      if (!orderMap[orderId]) {
        orderMap[orderId] = {
          order_id: orderId,
          booking_ref: row.booking_ref,
          created_at: row.created_at,
          status: row.status,
          overall_status: row.status || 'Created',
          eta: row.eta,
          etd: row.etd,
          collection_scope: row.collection_scope || '—',
          total_assigned_qty: row.total_assigned_qty || 0,
          sender: {
            name: row.sender_name || '—',
            contact: row.sender_contact || '—',
            email: row.sender_email || '—'
          },
          transport: {
            type: row.transport_type || '—',
            drop_method: row.drop_method || '—',
            delivery_date: row.delivery_date || '—'
          },
          receivers: {}
        };
      }

      const ord = orderMap[orderId];

      const recvKey = row.receiver_id ? row.receiver_id : `no_receiver_${orderId}`;

      if (!ord.receivers[recvKey]) {
        ord.receivers[recvKey] = {
          receiver_id: row.receiver_id || null,
          name: row.receiver_name || 'Unassigned',
          contact: row.receiver_contact || '—',
          email: row.receiver_email || '—',
          address: row.receiver_address || '—',
          status: row.receiver_status || '—',
          eta: row.receiver_eta || null,
          containers: row.receiver_containers || [],
          drop_off_details: row.drop_off_details || [],  // ← now aggregated JSON
          items: {}
        };
      }

      const recv = ord.receivers[recvKey];

      if (row.item_id) {
        if (!recv.items[row.item_id]) {
          recv.items[row.item_id] = {
            item_id: row.item_id,
            item_ref: row.item_ref || '—',
            category: row.category || '—',
            subcategory: row.subcategory || '—',
            type: row.type || '—',
            total_number: Number(row.total_number) || 0,
            weight: Number(row.weight) || 0,
            assignments: []
          };
        }

        if (row.assignment_id) {
          recv.items[row.item_id].assignments.push({
            assignment_id: row.assignment_id,
            container_number: row.container_number || '—',
            assigned_qty: Number(row.assigned_qty) || 0,
            status: row.assign_status || '—',
            created_at: row.assign_created_at,
            notes: row.assign_notes || ''
          });
        }
      }
    });

    const result = Object.values(orderMap).map(order => ({
      ...order,
      receivers: Object.values(order.receivers).map(receiver => ({
        ...receiver,
        items: Object.values(receiver.items)
      }))
    }));

    res.json({
      success: true,
      data: result.length === 1 ? result[0] : result,
      message: `Found ${result.length} matching order(s)`
    });

  } catch (err) {
    console.error('trackByOrderRef error:', err.stack || err);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch order details',
      details: process.env.NODE_ENV === 'development' ? err.message : undefined
    });
  }
}
// export async function getOrders(req, res) {
//   try {
//     const { page = 1, limit = 10, status, booking_ref, container_id } = req.query;
//     let whereClause = 'WHERE 1=1';
//     let params = [];
//     if (status) {
//       whereClause += ' AND o.status = $' + (params.length + 1);
//       params.push(status);
//     }
//     if (booking_ref) {
//       whereClause += ' AND o.booking_ref ILIKE $' + (params.length + 1);
//       params.push(`%${booking_ref}%`);
//     }
//     let containerNumbers = []; // To store looked-up container numbers
//     if (container_id) {
//       const containerIds = container_id.split(',').map(id => id.trim()).filter(Boolean);
//       if (containerIds.length > 0) {
//         // First, fetch container numbers for these CIDs
//         const idArray = containerIds.map(id => parseInt(id));
//         const containerQuery = {
//           text: 'SELECT container_number FROM container_master WHERE cid = ANY($1::int[])',
//           values: [idArray]
//         };
//         const containerResult = await pool.query(containerQuery);
//         containerNumbers = containerResult.rows.map(row => row.container_number).filter(Boolean);
//         if (containerNumbers.length === 0) {
//           // No containers found, early return empty
//           return res.json({
//             data: [],
//             pagination: {
//               page: parseInt(page),
//               limit: parseInt(limit),
//               total: 0,
//               totalPages: 0
//             }
//           });
//         }
//         // Conditions for ot.container_id (exact numeric match on CIDs)
//         const otConditions = containerIds.map((idStr) => {
//           const paramIdx = params.length + 1;
//           params.push(parseInt(idStr));
//           return `ot.container_id = $${paramIdx}`;
//         }).join(' OR ');
//         // Conditions for cm.container_number (partial ILIKE on looked-up numbers)
//         const cmConditions = containerNumbers.map((num) => {
//           const paramIdx = params.length + 1;
//           params.push(`%${num}%`);
//           return `cm.container_number ILIKE $${paramIdx}`;
//         }).join(' OR ');
//         // Conditions for receivers JSONB (partial ILIKE on looked-up numbers in unnested elements)
//         const receiverExists = containerNumbers.map((num) => {
//           const paramIdx = params.length + 1;
//           params.push(`%${num}%`);
//           return `EXISTS (
//             SELECT 1 FROM jsonb_array_elements_text(r.containers) AS cont
//             WHERE cont ILIKE $${paramIdx}
//           )`;
//         }).join(' OR ');
//         whereClause += ` AND (
//           (${otConditions}) OR
//           (${cmConditions}) OR
//           EXISTS (
//             SELECT 1 FROM receivers r
//             WHERE r.order_id = o.id
//             AND r.containers IS NOT NULL
//             AND (${receiverExists})
//           )
//         )`;
//       }
//     }
//     // Build SELECT fields dynamically (aligned with schema)
//     let selectFields = [
//       'o.*', // Core orders
//       's.sender_name, s.sender_contact, s.sender_address, s.sender_email, s.sender_ref, s.sender_type, s.selected_sender_owner', // From senders
//       't.transport_type, t.third_party_transport, t.driver_name, t.driver_contact, t.driver_nic', // From transport_details
//       't.driver_pickup_location, t.truck_number, t.drop_method, t.dropoff_name, t.drop_off_cnic',
//       't.drop_off_mobile, t.plate_no, t.drop_date, t.collection_method, t.collection_scope, t.qty_delivered', // From transport_details
//       't.client_receiver_name, t.client_receiver_id, t.client_receiver_mobile, t.delivery_date',
//       't.gatepass', // From transport_details
//       'ot.status AS tracking_status, ot.created_time AS tracking_created_time', // Latest tracking
//       'ot.container_id', // Explicit for join
//       'cm.container_number', // From container_master
//       // Fixed subquery for aggregated containers from all receivers (comma-separated unique container numbers)
//       'COALESCE((SELECT string_agg(DISTINCT elem, \', \') FROM (SELECT jsonb_array_elements_text(r3.containers) AS elem FROM receivers r3 WHERE r3.order_id = o.id AND r3.containers IS NOT NULL AND jsonb_array_length(r3.containers) > 0) AS unnested), \'\') AS receiver_containers_json',
//       // Updated subquery for full receivers as JSON array per order, now including nested order_items (formerly shippingDetails)
//       `(SELECT COALESCE(json_agg(
//         json_build_object(
//           'id', r2.id,
//           'order_id', r2.order_id,
//           'receiver_name', r2.receiver_name,
//           'receiver_contact', r2.receiver_contact,
//           'receiver_address', r2.receiver_address,
//           'receiver_email', r2.receiver_email,
//           'total_number', r2.total_number,
//           'total_weight', r2.total_weight,
//           'receiver_ref', r2.receiver_ref,
//           'remarks', r2.remarks,
//           'containers', r2.containers,
//           'status', r2.status,
//           'eta', r2.eta,
//           'etd', r2.etd,
//           'shipping_line', r2.shipping_line,
//           'consignment_vessel', r2.consignment_vessel,
//           'consignment_number', r2.consignment_number,
//           'consignment_marks', r2.consignment_marks,
//           'consignment_voyage', r2.consignment_voyage,
//           'full_partial', r2.full_partial,
//           'qty_delivered', r2.qty_delivered,
//           'shippingDetails', COALESCE((SELECT json_agg(row_to_json(oi.*)) FROM order_items oi WHERE oi.receiver_id = r2.id), '[]')
//         )
//       ), '[]') FROM receivers r2 WHERE r2.order_id = o.id) AS receivers`
//     ].join(', ');
//     // Build joins as array for easier extension (removed receivers join, now in subquery)
//     let joinsArray = [
//       'LEFT JOIN senders s ON o.id = s.order_id',
//       'LEFT JOIN transport_details t ON o.id = t.order_id',
//       'LEFT JOIN LATERAL (SELECT ot2.status, ot2.created_time, ot2.container_id FROM order_tracking ot2 WHERE ot2.order_id = o.id ORDER BY ot2.created_time DESC LIMIT 1) ot ON true', // Latest tracking
//       'LEFT JOIN container_master cm ON ot.container_id = cm.cid' // Join to container_master on cid
//     ];
//     const joins = joinsArray.join('\n ');
//     // For count, no need for subqueries or receivers join, but conditions on ot/cm/r are handled via the whereClause (which includes subqueries for r)
//     const countQuery = `
//       SELECT COUNT(DISTINCT o.id) as total_count
//       FROM orders o
//       ${joins}
//       ${whereClause}
//     `;
//     // Main query (no GROUP BY needed now with subqueries)
//     const query = `
//       SELECT ${selectFields}
//       FROM orders o
//       ${joins}
//       ${whereClause}
//       ORDER BY o.created_at DESC
//       LIMIT $${params.length + 1} OFFSET $${params.length + 2}
//     `;
//     params.push(parseInt(limit), (parseInt(page) - 1) * parseInt(limit));
//     const [result, countResult] = await Promise.all([
//       pool.query(query, params),
//       pool.query(countQuery, params.slice(0, -2)) // without limit offset
//     ]);
//     const total = parseInt(countResult.rows[0].total_count);
//     res.json({
//       data: result.rows,
//       pagination: {
//         page: parseInt(page),
//         limit: parseInt(limit),
//         total,
//         totalPages: Math.ceil(total / limit)
//       }
//     });
//   } catch (err) {
//     console.error("Error fetching orders:", err);
//     if (err.code === '42703') {
//       return res.status(500).json({ error: 'Database schema mismatch. Check table/column names in query.' });
//     }
//     res.status(500).json({ error: 'Failed to fetch orders', details: err.message });
//   }
// }

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
