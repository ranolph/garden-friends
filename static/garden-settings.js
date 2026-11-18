/* Garden Friends connection settings. Credentials remain in memory only. */
(() => {
  'use strict';

  const $ = (id) => document.getElementById(id);
  const groups = [
    {id: 'influx', title: 'InfluxDB', nav: 'Database', badge: 'Database', description: 'The database that stores your wildlife detections.', test: 'influx'},
    {id: 'mqtt', title: 'MQTT & BirdNET', nav: 'MQTT & audio', description: 'Receive detections from BirdNET-Go and your other connected detectors.', test: 'mqtt'},
    {id: 'frigate', title: 'Frigate', nav: 'Frigate', badge: 'Optional', description: 'Connect your camera detections and their snapshots.', test: 'frigate'},
    {id: 'homeassistant', title: 'Home Assistant', nav: 'Home Assistant', badge: 'Optional', description: 'Publish wildlife sensors through the same MQTT broker. Enable MQTT discovery in Home Assistant.', test: 'mqtt'},
    {id: 'display', title: 'Time & address', nav: 'Time & address', description: 'Choose the timezone and the address used for links to your garden.'},
    {id: 'advanced', title: 'Operation', nav: 'Operation', description: 'Control ingestion and how often Garden Friends refreshes its data.'}
  ];
  const compactKeys = new Set([
    'MQTT_PORT', 'INFLUX_TIMEOUT_MS', 'FRIGATE_MIN_SCORE', 'FRIGATE_MAMMAL_MIN_SCORE',
    'SUMMARY_INTERVAL', 'QUERY_CACHE_SECONDS'
  ]);
  const wideKeys = new Set([
    'INFLUX_URL', 'INFLUX_TOKEN', 'MQTT_PASSWORD', 'MQTT_CA_FILE', 'BIRDNET_TOPICS',
    'BIRDNET_MISSING_CONFIDENCE', 'FRIGATE_URLS', 'FRIGATE_AUTH_TOKEN', 'PUBLIC_BASE_URL', 'TIMEZONE'
  ]);
  const state = {
    session: null, revision: null, fields: new Map(), original: '', dirty: false,
    applying: false, activeTests: new Set(), requestSequence: 0
  };

  function element(tag, className, text) {
    const result = document.createElement(tag);
    if (className) result.className = className;
    if (text !== undefined) result.textContent = text;
    return result;
  }

  class RequestError extends Error {
    constructor(message, status = 0) { super(message); this.status = status; }
  }

  async function request(path, body, timeout = 15000) {
    const controller = new AbortController();
    const timer = window.setTimeout(() => controller.abort(), timeout);
    const options = {
      method: body === undefined ? 'GET' : 'POST', credentials: 'same-origin',
      cache: 'no-store', redirect: 'error', signal: controller.signal,
      headers: {'Accept': 'application/json'}
    };
    if (body !== undefined) {
      options.headers['Content-Type'] = 'application/json';
      options.headers['X-CSRF-Token'] = state.session?.csrf_token || '';
      options.body = JSON.stringify(body);
    }
    try {
      const response = await fetch(path, options);
      const contentType = response.headers.get('Content-Type') || '';
      if (!contentType.includes('application/json')) {
        throw new RequestError('Garden Friends returned an unexpected response. Try reloading this page.', response.status);
      }
      const data = await response.json();
      if (!response.ok) {
        throw new RequestError(typeof data.error === 'string' ? data.error : 'The request could not be completed.', response.status);
      }
      return data;
    } catch (error) {
      if (error instanceof RequestError) throw error;
      if (error.name === 'AbortError') throw new RequestError('The request timed out. Check that Garden Friends and the service are reachable.');
      throw new RequestError('Garden Friends could not be reached. Check your connection and try again.');
    } finally {
      window.clearTimeout(timer);
    }
  }

  function message(target, text = '', kind = '') {
    target.textContent = text;
    target.classList.toggle('is-error', kind === 'error');
    target.classList.toggle('is-success', kind === 'success');
    target.setAttribute('aria-live', kind === 'error' ? 'assertive' : 'polite');
    if (target.classList.contains('notice')) target.hidden = !text;
  }

  function show(view) {
    $('loading-state').hidden = view !== 'loading';
    $('auth-panel').hidden = view !== 'auth';
    $('settings-app').hidden = view !== 'settings';
    $('unavailable-panel').hidden = view !== 'unavailable';
  }

  function showAuth(note = '') {
    const setup = !!state.session?.setup_required;
    $('auth-title').textContent = setup ? 'Set up Garden Friends' : 'Connection settings';
    $('auth-description').textContent = setup
      ? 'Set an administrator password to configure Garden Friends.'
      : 'Sign in to manage your connections.';
    $('setup-form').hidden = !setup;
    $('login-form').hidden = setup;
    message($('auth-message'), note, note ? 'error' : '');
    show('auth');
  }

  function clearPrivateFields() {
    for (const record of state.fields.values()) {
      if (record.schema.type === 'password') record.input.value = '';
    }
    $('setup-form').reset();
    $('login-form').reset();
    $('password-form').reset();
  }

  async function recoverExpiredSession(error) {
    const expired = error.status === 401 && /session|sign.?in|authentication|required/i.test(error.message);
    if (!expired && !(error.status === 403 && /csrf|session|sign.?in/i.test(error.message))) return false;
    clearPrivateFields();
    state.dirty = false;
    state.session = await request('/api/settings/session');
    if (state.session.authenticated) {
      message($('page-message'), 'Your session changed. Please try that action again.', 'error');
    } else {
      showAuth('Your session expired. Sign in again to continue.');
    }
    return true;
  }

  function schemaGroup(field) {
    if (groups.some((group) => group.id === field.group)) return field.group;
    if (field.key.startsWith('INFLUX_')) return 'influx';
    if (field.key.startsWith('MQTT_') || field.key.startsWith('BIRDNET_')) return 'mqtt';
    if (field.key.startsWith('FRIGATE_')) return 'frigate';
    if (field.key.startsWith('HA_')) return 'homeassistant';
    if (['TIMEZONE', 'PUBLIC_BASE_URL'].includes(field.key)) return 'display';
    return 'advanced';
  }

  function booleanValue(value) {
    return value === true || value === 1 || value === '1' || value === 'true';
  }

  function createField(schema, value, hasSecret) {
    const id = 'setting-' + schema.key;
    const field = element('div', 'field');
    if (wideKeys.has(schema.key) || schema.type === 'textarea' || schema.type === 'password') field.classList.add('field-wide');
    if (compactKeys.has(schema.key)) field.classList.add('compact-field');
    const isBoolean = schema.type === 'boolean';
    const input = element(schema.type === 'textarea' ? 'textarea' : 'input');
    input.id = id;
    input.name = schema.key;
    if (input.tagName === 'INPUT') input.type = isBoolean ? 'checkbox' : ['url', 'number', 'password'].includes(schema.type) ? schema.type : 'text';
    if (isBoolean) input.checked = booleanValue(value);
    else input.value = schema.type === 'password' ? '' : String(value ?? '');
    input.autocomplete = schema.type === 'password' ? 'new-password' : 'off';
    if (!isBoolean) {
      input.spellcheck = false;
      input.setAttribute('autocapitalize', 'none');
    }
    if (schema.type === 'number') {
      input.required = true;
      input.step = String(schema.step ?? 'any');
      if (schema.min !== undefined) input.min = String(schema.min);
      if (schema.max !== undefined) input.max = String(schema.max);
    }
    if (typeof schema.placeholder === 'string') input.placeholder = schema.placeholder;
    if (schema.type === 'password') input.placeholder = hasSecret ? 'Leave blank to keep the saved value' : 'Not configured';
    const label = element('label', '', schema.label || schema.key);
    label.htmlFor = id;
    const record = {schema, input, clear: null, status: null, hasSecret: !!hasSecret};

    if (isBoolean) {
      field.classList.add('boolean-field');
      label.className = 'check-label';
      label.prepend(input);
      field.append(label);
    } else if (schema.type === 'password') {
      const row = element('div', 'field-label-row');
      const status = element('span', 'secret-state' + (hasSecret ? ' is-saved' : ''), hasSecret ? 'Saved' : 'Not set');
      status.id = id + '-state';
      row.append(label, status);
      field.append(row, input);
      record.status = status;
      input.setAttribute('aria-describedby', status.id);
    } else {
      field.append(label, input);
    }

    if (typeof schema.help === 'string' && schema.help) {
      const help = element('p', 'field-help', schema.help);
      help.id = id + '-help';
      input.setAttribute('aria-describedby', [input.getAttribute('aria-describedby'), help.id].filter(Boolean).join(' '));
      field.append(help);
    }

    if (schema.type === 'password') {
      const clear = element('input');
      clear.type = 'checkbox';
      clear.id = id + '-clear';
      const clearLabel = element('label', 'clear-secret');
      clearLabel.htmlFor = clear.id;
      clearLabel.append(clear, document.createTextNode('Remove the saved value'));
      clearLabel.hidden = !hasSecret;
      field.append(clearLabel);
      record.clear = clear;
      clear.addEventListener('change', () => {
        if (clear.checked) input.value = '';
        updateSecretStatus(record);
        markChanges();
      });
      input.addEventListener('input', () => {
        if (input.value) clear.checked = false;
        updateSecretStatus(record);
      });
    }
    input.addEventListener(isBoolean ? 'change' : 'input', markChanges);
    state.fields.set(schema.key, record);
    return field;
  }

  function updateSecretStatus(record) {
    const clearing = !!record.clear?.checked;
    record.status.textContent = clearing ? 'Will be removed' : record.input.value ? 'New value entered' : record.hasSecret ? 'Saved' : 'Not set';
    record.status.classList.toggle('is-saved', !clearing && (record.hasSecret || !!record.input.value));
    record.status.classList.toggle('is-clearing', clearing);
  }

  function collect() {
    const values = {};
    const clearSecrets = [];
    for (const [key, record] of state.fields) {
      if (record.schema.type === 'boolean') values[key] = record.input.checked;
      else if (record.schema.type === 'number') values[key] = record.input.value === '' ? '' : Number(record.input.value);
      else if (record.schema.type === 'password') {
        if (record.clear?.checked) clearSecrets.push(key);
        else if (record.input.value) values[key] = record.input.value;
      } else values[key] = record.input.value;
    }
    return {values, clear_secrets: clearSecrets};
  }

  function markChanges() {
    if (state.applying) return;
    state.dirty = JSON.stringify(collect()) !== state.original;
    document.querySelector('.save-bar').classList.toggle('has-changes', state.dirty);
    $('save-state').textContent = state.dirty ? 'Unsaved changes' : 'No unsaved changes';
    for (const node of document.querySelectorAll('.test-result')) message(node);
  }

  function updateModeSummary(values) {
    const mode = $('mode-summary');
    mode.replaceChildren();
    const readOnly = booleanValue(values.READ_ONLY);
    mode.append(element('strong', '', readOnly ? 'Read-only mode' : 'Ingestion enabled'));
    mode.append(document.createTextNode(readOnly ? 'Displays existing detections.' : 'Receives and stores new detections.'));
  }

  function renderSettings(data) {
    if (!data || typeof data.values !== 'object' || !Array.isArray(data.schema)) {
      throw new RequestError('The settings response was incomplete. Please reload this page.');
    }
    state.fields.clear();
    state.revision = data.revision;
    const container = $('settings-sections');
    const nav = $('section-navigation');
    container.replaceChildren();
    nav.replaceChildren();
    const fields = data.schema.filter((field) => field && /^[A-Z][A-Z0-9_]*$/.test(field.key));
    for (const group of groups) {
      const sectionFields = fields.filter((field) => schemaGroup(field) === group.id);
      if (!sectionFields.length) continue;
      const section = element('section', 'settings-section');
      section.id = 'section-' + group.id;
      section.setAttribute('aria-labelledby', 'heading-' + group.id);
      const heading = element('div', 'section-heading');
      const title = element('h2', '', group.title);
      title.id = 'heading-' + group.id;
      heading.append(title);
      if (group.badge) heading.append(element('span', 'section-badge', group.badge));
      section.append(heading, element('p', 'section-description', group.description));
      const grid = element('div', 'field-grid');
      for (const field of sectionFields) grid.append(createField(field, data.values[field.key], data.secrets?.[field.key]));
      section.append(grid);

      if (group.id === 'display') {
        const note = element('div', 'cutoff-note');
        const copy = element('div');
        copy.append(element('strong', '', 'Midnight day boundary'));
        copy.append(element('p', '', 'Each day begins at 00:00 in your selected timezone.'));
        note.append(element('span', 'cutoff-clock', '00:00'), copy);
        section.append(note);
      }
      if (group.test) {
        const actions = element('div', 'section-actions');
        const button = element('button', 'button button-secondary', group.id === 'homeassistant' ? 'Test shared MQTT broker' : 'Test connection');
        button.type = 'button';
        const result = element('p', 'inline-message test-result');
        result.setAttribute('role', 'status');
        result.setAttribute('aria-live', 'polite');
        button.addEventListener('click', () => testConnection(group, button, result));
        actions.append(button, result);
        section.append(actions);
      }
      container.append(section);
      const link = element('a', '', group.nav);
      link.href = '#' + section.id;
      nav.append(link);
    }
    if (!state.fields.size) throw new RequestError('No editable settings were returned. Please reload this page.');
    updateModeSummary(data.values);
    state.original = JSON.stringify(collect());
    state.dirty = false;
    state.applying = false;
    setApplying(false);
    markChanges();
  }

  function setApplying(applying) {
    state.applying = applying;
    $('connection-fields').disabled = applying;
    $('save-button').disabled = applying;
    $('reload-settings').disabled = applying;
    $('logout-button').disabled = applying;
    $('save-button').textContent = applying ? 'Applying…' : 'Save & apply';
    if (applying) $('save-state').textContent = 'Applying saved settings…';
  }

  async function loadSettings() {
    const data = await request('/api/settings');
    renderSettings(data);
    show('settings');
  }

  async function start(note = '') {
    const sequence = ++state.requestSequence;
    show('loading');
    try {
      const session = await request('/api/settings/session');
      if (sequence !== state.requestSequence) return;
      state.session = session;
      if (session.authenticated) await loadSettings();
      else showAuth(note);
    } catch (error) {
      if (sequence !== state.requestSequence) return;
      $('unavailable-message').textContent = error.message;
      show('unavailable');
    }
  }

  async function authenticate(form, endpoint, payload) {
    const submit = form.querySelector('button[type=submit]');
    submit.disabled = true;
    message($('auth-message'));
    try {
      state.session = await request(endpoint, payload);
      form.reset();
      await loadSettings();
      $('connection-form').querySelector('input, textarea')?.focus();
    } catch (error) {
      message($('auth-message'), error.message, 'error');
      // A successful password check may have rotated the CSRF token before a
      // transient settings read failed. Refresh it before another submission.
      try {
        state.session = await request('/api/settings/session');
        if (state.session.authenticated) {
          $('unavailable-message').textContent = error.message;
          show('unavailable');
        } else showAuth(error.message);
      } catch (_) { /* Keep the original error visible. */ }
    } finally {
      submit.disabled = false;
    }
  }

  $('setup-form').addEventListener('submit', (event) => {
    event.preventDefault();
    $('setup-confirm').setCustomValidity($('setup-password').value === $('setup-confirm').value ? '' : 'The passwords do not match.');
    if (!$('setup-form').reportValidity()) return;
    authenticate($('setup-form'), '/api/settings/setup', {
      setup_code: $('setup-code').value.trim(), password: $('setup-password').value
    });
  });
  for (const id of ['setup-password', 'setup-confirm']) $(id).addEventListener('input', () => $('setup-confirm').setCustomValidity(''));
  $('login-form').addEventListener('submit', (event) => {
    event.preventDefault();
    if (!$('login-form').reportValidity()) return;
    authenticate($('login-form'), '/api/settings/login', {password: $('login-password').value});
  });

  async function testConnection(group, button, result) {
    if (state.applying || state.activeTests.has(group.id)) return;
    if (!$('connection-form').reportValidity()) return;
    state.activeTests.add(group.id);
    button.disabled = true;
    message(result, 'Checking connection…');
    try {
      const draft = collect();
      const fingerprint = JSON.stringify(draft);
      const answer = await request('/api/settings/test', {connection: group.test, ...draft}, 30000);
      if (JSON.stringify(collect()) !== fingerprint) {
        message(result, 'Settings changed during this check. Test again to check the new values.');
        return;
      }
      const prefix = group.id === 'homeassistant' && answer.ok ? 'Shared broker: ' : '';
      message(result, prefix + (answer.message || (answer.ok ? 'Connection successful.' : 'The connection could not be verified.')), answer.ok ? 'success' : 'error');
    } catch (error) {
      try {
        if (!await recoverExpiredSession(error)) message(result, error.message, 'error');
      } catch (_) { message(result, error.message, 'error'); }
    } finally {
      state.activeTests.delete(group.id);
      button.disabled = false;
    }
  }

  async function waitForRevision(revision) {
    const deadline = Date.now() + 65000;
    await new Promise((resolve) => window.setTimeout(resolve, 1200));
    while (Date.now() < deadline) {
      try {
        const health = await request('/api/health', undefined, 5000);
        if (health.status === 'running' && health.settings_revision !== undefined && String(health.settings_revision) === String(revision)) return true;
      } catch (_) { /* A brief disconnect is expected while the worker restarts. */ }
      await new Promise((resolve) => window.setTimeout(resolve, 1200));
    }
    return false;
  }

  $('connection-form').addEventListener('submit', async (event) => {
    event.preventDefault();
    if (state.applying || !$('connection-form').reportValidity()) return;
    const draft = collect();
    setApplying(true);
    message($('page-message'));
    try {
      const result = await request('/api/settings/save', {...draft, revision: state.revision});
      state.revision = result.revision;
      state.dirty = false;
      document.querySelector('.save-bar').classList.remove('has-changes');
      clearPrivateFields();
      for (const [key, record] of state.fields) {
        if (record.schema.type !== 'password') continue;
        if (draft.clear_secrets.includes(key)) record.hasSecret = false;
        else if (draft.values[key]) record.hasSecret = true;
        record.clear.checked = false;
        record.clear.parentElement.hidden = !record.hasSecret;
        updateSecretStatus(record);
      }
      state.original = JSON.stringify(collect());
      message($('page-message'), 'Settings saved. Waiting for Garden Friends to apply them…');
      const applied = result.restarting === false || await waitForRevision(result.revision);
      if (applied) {
        await loadSettings();
        message($('page-message'), 'Settings saved and applied.', 'success');
      } else {
        message($('page-message'), 'Settings were saved, but the restart has not been confirmed. Use Reload to check again.', 'error');
        $('save-state').textContent = 'Saved; application not yet confirmed';
      }
    } catch (error) {
      try {
        if (!await recoverExpiredSession(error)) message($('page-message'), error.message, 'error');
      } catch (_) { message($('page-message'), error.message, 'error'); }
    } finally {
      setApplying(false);
      if (state.dirty) $('save-state').textContent = 'Unsaved changes';
    }
  });

  $('reload-settings').addEventListener('click', async () => {
    if (state.dirty && !window.confirm('Discard your unsaved changes and reload the saved settings?')) return;
    $('reload-settings').disabled = true;
    try {
      state.session = await request('/api/settings/session');
      if (!state.session.authenticated) {
        clearPrivateFields();
        state.dirty = false;
        showAuth('Sign in again to load your settings.');
        return;
      }
      await loadSettings();
      message($('page-message'), 'Saved settings reloaded.');
    } catch (error) {
      try {
        if (!await recoverExpiredSession(error)) message($('page-message'), error.message, 'error');
      } catch (_) { message($('page-message'), error.message, 'error'); }
    } finally { $('reload-settings').disabled = false; }
  });

  $('logout-button').addEventListener('click', async () => {
    if (state.dirty && !window.confirm('Sign out and discard your unsaved changes?')) return;
    $('logout-button').disabled = true;
    try {
      state.session = await request('/api/settings/logout', {});
      clearPrivateFields();
      state.dirty = false;
      state.fields.clear();
      $('settings-sections').replaceChildren();
      await start();
    } catch (error) {
      message($('page-message'), error.message, 'error');
    } finally { $('logout-button').disabled = false; }
  });

  $('password-form').addEventListener('submit', async (event) => {
    event.preventDefault();
    const form = $('password-form');
    $('confirm-password').setCustomValidity($('new-password').value === $('confirm-password').value ? '' : 'The passwords do not match.');
    if (!form.reportValidity()) return;
    const submit = form.querySelector('button[type=submit]');
    submit.disabled = true;
    message($('password-message'));
    try {
      const result = await request('/api/settings/password', {
        current_password: $('current-password').value, password: $('new-password').value
      });
      state.session = typeof result.csrf_token === 'string' ? {...state.session, ...result} : await request('/api/settings/session');
      form.reset();
      message($('password-message'), 'Administrator password updated.', 'success');
      if (!state.session.authenticated) showAuth();
    } catch (error) {
      try {
        if (!await recoverExpiredSession(error)) message($('password-message'), error.message, 'error');
      } catch (_) { message($('password-message'), error.message, 'error'); }
    } finally { submit.disabled = false; }
  });
  for (const id of ['new-password', 'confirm-password']) $(id).addEventListener('input', () => $('confirm-password').setCustomValidity(''));

  $('retry-button').addEventListener('click', () => start());
  window.addEventListener('beforeunload', (event) => {
    if (!state.dirty) return;
    event.preventDefault();
    event.returnValue = '';
  });
  start();
})();
