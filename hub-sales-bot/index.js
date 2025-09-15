// Unity Auto LeadBot — Node 16, CommonJS
require('dotenv').config();
const { Telegraf, Markup } = require('telegraf');
const fetch = require('node-fetch');
const { ulid } = require('ulid');
const dayjs = require('dayjs');
const fs = require('fs');
const path = require('path');
const pino = require('pino');

// ----------------- Логгер -----------------
const LOG = pino({ level: process.env.LOG_LEVEL || 'info' });

// ----------------- Пути и файлы -----------------
const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
const OUTBOX_PATH = path.join(DATA_DIR, 'outbox.json');
const DEDUP_PATH = path.join(DATA_DIR, 'dedup.json');
const RR_PATH = path.join(DATA_DIR, 'rr.json');
const FAQ_PATH = path.join(__dirname, 'faq.json');

// ----------------- Конфиг окружения -----------------
const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_CHAT_ID = Number(process.env.ADMIN_CHAT_ID || 0);
const VIN_BOT_URL = process.env.VIN_BOT_URL;
const NEWS_CHANNEL_URL = process.env.NEWS_CHANNEL_URL;

// amoCRM
const AMO_BASE_URL = process.env.AMO_BASE_URL;
const AMO_CLIENT_ID = process.env.AMO_CLIENT_ID;
const AMO_CLIENT_SECRET = process.env.AMO_CLIENT_SECRET;
const AMO_REDIRECT_URI = process.env.AMO_REDIRECT_URI;
const AMO_REFRESH_TOKEN = process.env.AMO_REFRESH_TOKEN;

// Пайплайны/статусы
const AMO_PIPELINE_IMPORT_ID = Number(process.env.AMO_PIPELINE_IMPORT_ID || 0);
const AMO_PIPELINE_SALON_ID = Number(process.env.AMO_PIPELINE_SALON_ID || 0);
const AMO_STATUS_IMPORT_NEW_ID = Number(process.env.AMO_STATUS_IMPORT_NEW_ID || 0);
const AMO_STATUS_SALON_NEW_ID = Number(process.env.AMO_STATUS_SALON_NEW_ID || 0);

// Кастомные поля LEAD
const CF = {
  DIRECTION: Number(process.env.AMO_CF_LEAD_DIRECTION_ID || 0),
  COUNTRY: Number(process.env.AMO_CF_LEAD_COUNTRY_ID || 0),
  DELIVERY_CITY: Number(process.env.AMO_CF_LEAD_DELIVERY_CITY_ID || 0),
  CITY: Number(process.env.AMO_CF_LEAD_CITY_ID || 0),
  BUDGET_MIN: Number(process.env.AMO_CF_LEAD_BUDGET_MIN_ID || 0),
  BUDGET_MAX: Number(process.env.AMO_CF_LEAD_BUDGET_MAX_ID || 0),
  CURRENCY: Number(process.env.AMO_CF_LEAD_CURRENCY_ID || 0),
  BODY: Number(process.env.AMO_CF_LEAD_BODY_ID || 0),
  TRANSMISSION: Number(process.env.AMO_CF_LEAD_TRANSMISSION_ID || 0),
  DRIVE: Number(process.env.AMO_CF_LEAD_DRIVE_ID || 0),
  FUEL: Number(process.env.AMO_CF_LEAD_FUEL_ID || 0),
  START_PARAM: Number(process.env.AMO_CF_LEAD_START_PARAM_ID || 0),
  UTM_SOURCE: Number(process.env.AMO_CF_LEAD_UTM_SOURCE_ID || 0),
  UTM_MEDIUM: Number(process.env.AMO_CF_LEAD_UTM_MEDIUM_ID || 0),
  UTM_CAMPAIGN: Number(process.env.AMO_CF_LEAD_UTM_CAMPAIGN_ID || 0),
  UTM_CONTENT: Number(process.env.AMO_CF_LEAD_UTM_CONTENT_ID || 0),
  PD_VERSION: Number(process.env.AMO_CF_LEAD_PD_VERSION_ID || 0),
  PD_TS: Number(process.env.AMO_CF_LEAD_PD_TS_ID || 0)
};

// Кастомное поле CONTACT (Telegram) — опционально
const AMO_CF_CONTACT_TELEGRAM_ID = Number(process.env.AMO_CF_CONTACT_TELEGRAM_ID || 0);

// Роутинг
const RESPONSIBLES = String(process.env.AMO_RESPONSIBLES || '')
  .split(',')
  .map(s => Number(s.trim()))
  .filter(Boolean);

// ----------------- Состояния/хранилища -----------------
const S = new Map(); // состояния пользователей
const outbox = loadJSON(OUTBOX_PATH, []); // очередь доставок
const dedup  = loadJSON(DEDUP_PATH, {});  // { "+7999...": "2025-09-12T..." }
const rr     = loadJSON(RR_PATH, { i: 0 }); // round-robin

// ----------------- FAQ загрузка и индексы -----------------
const FAQ_PAGE_SIZE = 8;

const FAQ = loadJSON(FAQ_PATH, { sections: [] });
const FAQIndex = buildFaqIndex(FAQ);

function buildFaqIndex(faq) {
  const secById = new Map();
  const subByKey = new Map(); // `${secId}:${subId}` -> sub
  const qById = new Map();    // qid -> {secId, subId, q, a}
  for (const sec of faq.sections || []) {
    secById.set(sec.id, sec);
    for (const sub of sec.subs || []) {
      subByKey.set(`${sec.id}:${sub.id}`, sub);
      for (const q of sub.qs || []) {
        qById.set(q.id, { ...q, secId: sec.id, subId: sub.id, secTitle: sec.title, subTitle: sub.title });
      }
    }
  }
  return { secById, subByKey, qById };
}

// ----------------- Утилиты -----------------
function loadJSON(file, fallback) {
  try {
    if (fs.existsSync(file)) return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch (e) { LOG.warn({ e }, `loadJSON error: ${file}`); }
  return fallback;
}
function saveJSON(file, data) { fs.writeFileSync(file, JSON.stringify(data, null, 2)); }
function nowISO() { return new Date().toISOString(); }
function normalizePhoneE164(raw) {
  if (!raw) return null;
  let p = String(raw).replace(/[^\d+]/g, '');
  if (p.startsWith('8') && p.length === 11) p = '+7' + p.slice(1);
  if (!p.startsWith('+')) p = '+' + p;
  return p;
}
function nextResponsible() {
  if (!RESPONSIBLES.length) return null;
  const id = RESPONSIBLES[rr.i % RESPONSIBLES.length];
  rr.i = (rr.i + 1) % RESPONSIBLES.length;
  saveJSON(RR_PATH, rr);
  return id;
}
function fmt(n) { return (n || 0).toLocaleString('ru-RU'); }
function humanBudget(b) {
  if (!b) return '—';
  if (b.min != null && b.max != null) return `${fmt(b.min)}–${fmt(b.max)} ₽`;
  if (b.min != null && b.max == null) return `от ${fmt(b.min)} ₽`;
  return '—';
}
function parseUTM(startParam) {
  const res = { source: '', medium: '', campaign: '', content: '' };
  if (!startParam) return res;
  const parts = startParam.split('~');
  for (const p of parts) {
    const [k, v] = p.split(':');
    if (k === 'utm_source') res.source = v;
    if (k === 'utm_medium') res.medium = v;
    if (k === 'utm_campaign') res.campaign = v;
    if (k === 'utm_content') res.content = v;
  }
  return res;
}

// ----------------- AmoCRM клиент -----------------
const amo = (() => {
  async function refreshToken() {
    const url = `${AMO_BASE_URL}/oauth2/access_token`;
    const body = {
      client_id: AMO_CLIENT_ID,
      client_secret: AMO_CLIENT_SECRET,
      grant_type: 'refresh_token',
      refresh_token: AMO_REFRESH_TOKEN,
      redirect_uri: AMO_REDIRECT_URI
    };
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
      body: JSON.stringify(body)
    });
    if (!res.ok) throw new Error(`AMO refresh failed: ${res.status} ${await res.text()}`);
    const data = await res.json();
    amo.accessToken = data.access_token;
    amo.expiresAt = Date.now() + (data.expires_in - 60) * 1000;
    LOG.info('amo: token refreshed');
  }

  async function ensureToken() {
    if (!amo.accessToken || Date.now() > amo.expiresAt) await refreshToken();
  }

  async function api(method, path, body) {
    await ensureToken();
    const url = `${AMO_BASE_URL}${path}`;
    const res = await fetch(url, {
      method,
      headers: {
        'Authorization': `Bearer ${amo.accessToken}`,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      body: body ? JSON.stringify(body) : undefined
    });
    if (!res.ok) throw new Error(`AMO ${method} ${path} -> ${res.status}: ${await res.text()}`);
    if (res.status === 204) return null;
    return res.json();
  }

  async function findContactByPhone(phone) {
    const q = encodeURIComponent(phone);
    const data = await api('GET', `/api/v4/contacts?limit=1&query=${q}`);
    return (data && data._embedded && data._embedded.contacts && data._embedded.contacts[0]) || null;
  }

  function cfValue(field_id, value) { return { field_id, values: [{ value }] }; }

  async function createOrUpdateContact({ phone_e164, name, tg_username }) {
    const found = await findContactByPhone(phone_e164);
    const contactCF = [];
    if (Number(process.env.AMO_CF_CONTACT_TELEGRAM_ID || 0) && tg_username) {
      contactCF.push(cfValue(Number(process.env.AMO_CF_CONTACT_TELEGRAM_ID), `@${tg_username}`));
    }

    if (found) {
      const body = [{
        id: found.id,
        name: name || found.name || phone_e164,
        ...(contactCF.length ? { custom_fields_values: contactCF } : {})
      }];
      await api('PATCH', `/api/v4/contacts`, body);
      return found.id;
    } else {
      const body = [{
        name: name || phone_e164,
        custom_fields_values: [
          { field_code: 'PHONE', values: [{ value: phone_e164 }] },
          ...contactCF
        ]
      }];
      const resp = await api('POST', `/api/v4/contacts`, body);
      return resp._embedded.contacts[0].id;
    }
  }

  async function createLead({ flow, country, delivery_city, city, budget, prefs, start_param, utm, consent, responsible_id }) {
    const pipeline_id = flow === 'import' ? AMO_PIPELINE_IMPORT_ID : AMO_PIPELINE_SALON_ID;
    const status_id   = flow === 'import' ? AMO_STATUS_IMPORT_NEW_ID : AMO_STATUS_SALON_NEW_ID;

    const cfs = [];
    if (CF.DIRECTION)   cfs.push(cfValue(CF.DIRECTION, flow));
    if (flow === 'import' && CF.COUNTRY && country) cfs.push(cfValue(CF.COUNTRY, country));
    if (flow === 'import' && CF.DELIVERY_CITY && delivery_city) cfs.push(cfValue(CF.DELIVERY_CITY, delivery_city));
    if (flow === 'salon'  && CF.CITY && city) cfs.push(cfValue(CF.CITY, city));

    if (budget) {
      if (CF.BUDGET_MIN && budget.min != null) cfs.push(cfValue(CF.BUDGET_MIN, budget.min));
      if (CF.BUDGET_MAX && budget.max != null) cfs.push(cfValue(CF.BUDGET_MAX, budget.max));
      if (CF.CURRENCY   && budget.currency)     cfs.push(cfValue(CF.CURRENCY, budget.currency));
    }
    if (prefs) {
      if (CF.BODY && prefs.body)             cfs.push(cfValue(CF.BODY, prefs.body));
      if (CF.TRANSMISSION && prefs.transmission) cfs.push(cfValue(CF.TRANSMISSION, prefs.transmission));
      if (CF.DRIVE && prefs.drive)           cfs.push(cfValue(CF.DRIVE, prefs.drive));
      if (CF.FUEL && prefs.fuel)             cfs.push(cfValue(CF.FUEL, prefs.fuel));
    }
    if (CF.START_PARAM && start_param) cfs.push(cfValue(CF.START_PARAM, start_param));
    if (utm) {
      if (CF.UTM_SOURCE && utm.source)   cfs.push(cfValue(CF.UTM_SOURCE, utm.source));
      if (CF.UTM_MEDIUM && utm.medium)   cfs.push(cfValue(CF.UTM_MEDIUM, utm.medium));
      if (CF.UTM_CAMPAIGN && utm.campaign) cfs.push(cfValue(CF.UTM_CAMPAIGN, utm.campaign));
      if (CF.UTM_CONTENT && utm.content) cfs.push(cfValue(CF.UTM_CONTENT, utm.content));
    }
    if (consent && CF.PD_VERSION && CF.PD_TS) {
      cfs.push(cfValue(CF.PD_VERSION, consent.version || ''));
      cfs.push(cfValue(CF.PD_TS, consent.ts || ''));
    }

    const name = flow === 'import'
      ? `Импорт ${country || ''} • ${delivery_city || ''} • ${humanBudget(budget)}`
      : `Салон • ${city || ''} • ${humanBudget(budget)}`;

    const body = [{
      name,
      pipeline_id,
      status_id,
      ...(responsible_id ? { responsible_user_id: responsible_id } : {}),
      custom_fields_values: cfs
    }];

    const resp = await api('POST', `/api/v4/leads`, body);
    return resp._embedded.leads[0].id;
  }

  async function linkContactToLead(contact_id, lead_id) {
    const body = [{ to_entity_id: contact_id, to_entity_type: 'contacts' }];
    await api('POST', `/api/v4/leads/${lead_id}/link`, body);
  }

  async function addTask(lead_id, text, completeAtTs) {
    const body = [{
      text,
      complete_till: Math.floor(completeAtTs / 1000),
      entity_id: lead_id,
      entity_type: 'leads',
      task_type_id: 1
    }];
    await api('POST', `/api/v4/tasks`, body);
  }

  async function addNote(lead_id, text) {
    const body = [{ note_type: 'common', params: { text } }];
    await api('POST', `/api/v4/leads/${lead_id}/notes`, body);
  }

  return { createOrUpdateContact, createLead, linkContactToLead, addTask, addNote, accessToken: null, expiresAt: 0 };
})();

// ----------------- Telegram бот -----------------
if (!BOT_TOKEN) { LOG.error('BOT_TOKEN is missing'); process.exit(1); }
const bot = new Telegraf(BOT_TOKEN);

// --------- Константы UI ----------
const COUNTRIES = [
  { code: 'CN', flag: '🇨🇳', title: 'Китай' },
  { code: 'KR', flag: '🇰🇷', title: 'Корея' },
  { code: 'AE', flag: '🇦🇪', title: 'ОАЭ' },
  { code: 'US', flag: '🇺🇸', title: 'США' },
  { code: 'EU', flag: '🇪🇺', title: 'Европа' }
];

const PRESET_CITIES = [
  { k: 'msk', t: 'Москва' },
  { k: 'spb', t: 'Санкт-Петербург' },
  { k: 'ekb', t: 'Екатеринбург' },
  { k: 'nsk', t: 'Новосибирск' },
  { k: 'oth', t: 'Другой…' }
];

const PRESET_BUDGETS = [
  { k: 'a', t: 'До 1.5 млн', min: 0, max: 1500000 },
  { k: 'b', t: '1.5–3 млн', min: 1500000, max: 3000000 },
  { k: 'c', t: '3–5 млн', min: 3000000, max: 5000000 },
  { k: 'd', t: '5+ млн', min: 5000000, max: null },
  { k: 'x', t: 'Укажу сам', custom: true }
];

function chips(s) {
  const arr = [];
  if (s.flow === 'import' && s.country) {
    const c = COUNTRIES.find(x => x.code === s.country);
    if (c) arr.push(`Страна: ${c.flag} ${c.title}`);
  }
  if (s.flow === 'import' && s.delivery_city) arr.push(`Доставка: ${s.delivery_city}`);
  if (s.flow === 'salon' && s.city) arr.push(`Город: ${s.city}`);
  if (s.budget) arr.push(`Бюджет: ${humanBudget(s.budget)}`);
  if (s.prefs) {
    if (s.prefs.body) arr.push(`Кузов: ${s.prefs.body}`);
    if (s.prefs.transmission) arr.push(`КПП: ${s.prefs.transmission}`);
    if (s.prefs.drive) arr.push(`Привод: ${s.prefs.drive}`);
    if (s.prefs.fuel) arr.push(`Топливо: ${s.prefs.fuel}`);
  }
  if (s.contact_method) arr.push(`Связь: ${s.contact_method}`);
  return arr.length ? `\n\n${arr.map(x => `• ${x}`).join('\n')}` : '';
}

function kbMain() {
  return Markup.inlineKeyboard([
    [Markup.button.callback('Подбор из салона', 'flow:salon'), Markup.button.callback('Импорт из 5 стран', 'flow:import')],
    [Markup.button.url('Проверка по VIN', VIN_BOT_URL || 'https://t.me')],
    [Markup.button.url('Новостной канал', NEWS_CHANNEL_URL || 'https://t.me')],
    [Markup.button.callback('Ответы на вопросы', 'faq')]
  ]);
}

// ----------------- Рендер мастера -----------------
function renderImportPage(s) {
  const step = s.step || 1;
  let text = `🚗 Импорт авто • Шаг ${step}/6${chips(s)}\n`;
  let rows = [];

  if (step === 1) {
    text += '\nВыберите страну:';
    rows = [
      COUNTRIES.slice(0,3).map(c => Markup.button.callback(`${c.flag} ${c.title}`, `imp:country:${c.code}`)),
      COUNTRIES.slice(3).map(c => Markup.button.callback(`${c.flag} ${c.title}`, `imp:country:${c.code}`)),
      [Markup.button.callback('В меню', 'home')]
    ];
  }

  if (step === 2) {
    text += '\nГород доставки:';
    rows = [
      PRESET_CITIES.slice(0,3).map(c => Markup.button.callback(c.t, `imp:dc:${c.k}`)),
      PRESET_CITIES.slice(3).map(c => Markup.button.callback(c.t, `imp:dc:${c.k}`)),
      [Markup.button.callback('↩ Назад', 'imp:back'), Markup.button.callback('В меню', 'home')]
    ];
  }

  if (step === 3) {
    text += '\nБюджет:';
    rows = [
      PRESET_BUDGETS.slice(0,2).map(b => Markup.button.callback(b.t, `imp:bud:${b.k}`)),
      PRESET_BUDGETS.slice(2,4).map(b => Markup.button.callback(b.t, `imp:bud:${b.k}`)),
      [Markup.button.callback('Укажу сам', 'imp:bud:x')],
      [Markup.button.callback('↩ Назад', 'imp:back'), Markup.button.callback('В меню', 'home')]
    ];
  }

  if (step === 4) {
    text += '\nПредпочтения (переключатели):';
    const on = (cur, val) => cur === val ? '✅' : '□';
    rows = [
      [Markup.button.callback(`${on(s.prefs?.body,'седан')} Седан`, 'imp:pref:body:седан'),
       Markup.button.callback(`${on(s.prefs?.body,'кроссовер')} Кросс`, 'imp:pref:body:кроссовер')],
      [Markup.button.callback(`${on(s.prefs?.transmission,'AT')} КПП AT`, 'imp:pref:gear:AT'),
       Markup.button.callback(`${on(s.prefs?.transmission,'MT')} КПП MT`, 'imp:pref:gear:MT')],
      [Markup.button.callback(`${on(s.prefs?.drive,'FWD')} Привод FWD`, 'imp:pref:drive:FWD'),
       Markup.button.callback(`${on(s.prefs?.drive,'AWD')} Привод AWD`, 'imp:pref:drive:AWD')],
      [Markup.button.callback(`${on(s.prefs?.fuel,'бензин')} Бензин`, 'imp:pref:fuel:бензин'),
       Markup.button.callback(`${on(s.prefs?.fuel,'гибрид')} Гибрид`, 'imp:pref:fuel:гибрид'),
       Markup.button.callback(`${on(s.prefs?.fuel,'электро')} Электро`, 'imp:pref:fuel:электро')],
      [Markup.button.callback('↩ Назад', 'imp:back'), Markup.button.callback('Далее →', 'imp:next')]
    ];
  }

  if (step === 5) {
    text += '\nКак связаться?';
    rows = [
      [Markup.button.callback('Telegram', 'imp:cm:tg'), Markup.button.callback('WhatsApp', 'imp:cm:wa'), Markup.button.callback('Звонок', 'imp:cm:call')],
      [Markup.button.callback('↩ Назад', 'imp:back'), Markup.button.callback('Далее →', 'imp:next')]
    ];
  }

  if (step === 6) {
    text += '\nПодтвердите телефон (кнопкой ниже).';
    rows = [
      [Markup.button.callback('Отправить телефон', 'imp:contact')],
      [Markup.button.callback('↩ Назад', 'imp:back'), Markup.button.callback('В меню', 'home')]
    ];
  }

  return { text, markup: Markup.inlineKeyboard(rows) };
}

function renderSalonPage(s) {
  const step = s.step || 1;
  let text = `🚘 Подбор из салона • Шаг ${step}/5${chips(s)}\n`;
  let rows = [];

  if (step === 1) {
    text += '\nВаш город:';
    rows = [
      PRESET_CITIES.slice(0,3).map(c => Markup.button.callback(c.t, `sal:city:${c.k}`)),
      PRESET_CITIES.slice(3).map(c => Markup.button.callback(c.t, `sal:city:${c.k}`)),
      [Markup.button.callback('В меню', 'home')]
    ];
  }

  if (step === 2) {
    text += '\nБюджет:';
    rows = [
      PRESET_BUDGETS.slice(0,2).map(b => Markup.button.callback(b.t, `sal:bud:${b.k}`)),
      PRESET_BUDGETS.slice(2,4).map(b => Markup.button.callback(b.t, `sal:bud:${b.k}`)),
      [Markup.button.callback('Укажу сам', 'sal:bud:x')],
      [Markup.button.callback('↩ Назад', 'sal:back'), Markup.button.callback('Вперёд →', 'sal:next')]
    ];
  }

  if (step === 3) {
    text += '\nМини-предпочтения:';
    const on = (cur, val) => cur === val ? '✅' : '□';
    rows = [
      [Markup.button.callback(`${on(s.prefs?.body,'седан')} Седан`, 'sal:pref:body:седан'),
       Markup.button.callback(`${on(s.prefs?.body,'кроссовер')} Кросс`, 'sal:pref:body:кроссовер')],
      [Markup.button.callback(`${on(s.prefs?.fuel,'бензин')} Бензин`, 'sal:pref:fuel:бензин'),
       Markup.button.callback(`${on(s.prefs?.fuel,'гибрид')} Гибрид`, 'sal:pref:fuel:гибрид')],
      [Markup.button.callback('↩ Назад', 'sal:back'), Markup.button.callback('Далее →', 'sal:next')]
    ];
  }

  if (step === 4) {
    text += '\nКак связаться?';
    rows = [
      [Markup.button.callback('Telegram', 'sal:cm:tg'), Markup.button.callback('WhatsApp', 'sal:cm:wa'), Markup.button.callback('Звонок', 'sal:cm:call')],
      [Markup.button.callback('↩ Назад', 'sal:back'), Markup.button.callback('Далее →', 'sal:next')]
    ];
  }

  if (step === 5) {
    text += '\nПодтвердите телефон:';
    rows = [
      [Markup.button.callback('Отправить телефон', 'sal:contact')],
      [Markup.button.callback('↩ Назад', 'sal:back'), Markup.button.callback('В меню', 'home')]
    ];
  }

  return { text, markup: Markup.inlineKeyboard(rows) };
}

// ----------------- FAQ рендеры -----------------
function renderFaqSections() {
  const rows = [];
  const secs = FAQ.sections || [];
  for (const sec of secs) rows.push([Markup.button.callback(sec.title, `faq:sec:${sec.id}`)]);
  rows.push([Markup.button.callback('В меню', 'home')]);
  const text = '❓ Разделы ответов на вопросы:';
  return { text, markup: Markup.inlineKeyboard(rows) };
}

function renderFaqSubs(secId) {
  const sec = FAQIndex.secById.get(secId);
  if (!sec) return { text: 'Раздел не найден.', markup: Markup.inlineKeyboard([[Markup.button.callback('К разделам', 'faq')]]) };
  const rows = [];
  for (const sub of sec.subs || []) rows.push([Markup.button.callback(sub.title, `faq:sub:${secId}:${sub.id}`)]);
  rows.push([Markup.button.callback('↩ К разделам', 'faq')]);
  const text = `❓ ${sec.title}\nВыберите подраздел:`;
  return { text, markup: Markup.inlineKeyboard(rows) };
}

function renderFaqQuestions(secId, subId, page = 0) {
  const key = `${secId}:${subId}`;
  const sub = FAQIndex.subByKey.get(key);
  const sec = FAQIndex.secById.get(secId);
  if (!sub || !sec) return { text: 'Подраздел не найден.', markup: Markup.inlineKeyboard([[Markup.button.callback('↩ К подразделам', `faq:sec:${secId}`)]]) };

  const qs = sub.qs || [];
  const total = qs.length;
  const pages = Math.max(1, Math.ceil(total / FAQ_PAGE_SIZE));
  const p = Math.min(Math.max(0, page), pages - 1);
  const from = p * FAQ_PAGE_SIZE;
  const to = Math.min(from + FAQ_PAGE_SIZE, total);

  const rows = [];
  qs.slice(from, to).forEach(q => rows.push([Markup.button.callback(q.q, `faq:q:${q.id}`)]));

  const nav = [];
  if (p > 0) nav.push(Markup.button.callback('← Назад', `faq:list:${secId}:${subId}:${p - 1}`));
  if (p < pages - 1) nav.push(Markup.button.callback('Вперёд →', `faq:list:${secId}:${subId}:${p + 1}`));
  if (nav.length) rows.push(nav);

  rows.push([Markup.button.callback('↩ К подразделам', `faq:sec:${secId}`), Markup.button.callback('К разделам', 'faq')]);

  const text = `❓ ${sec.title} → ${sub.title}\nВыберите вопрос:`;
  return { text, markup: Markup.inlineKeyboard(rows) };
}

function renderFaqAnswer(qid) {
  const q = FAQIndex.qById.get(qid);
  if (!q) return { text: 'Вопрос не найден.', markup: Markup.inlineKeyboard([[Markup.button.callback('К разделам', 'faq')]]) };
  const text = `❓ ${q.secTitle} → ${q.subTitle}\n**${q.q}**\n\n${q.a}`;
  const rows = [
    [Markup.button.callback('↩ К вопросам', `faq:list:${q.secId}:${q.subId}:0`)],
    [Markup.button.callback('К подразделу', `faq:sub:${q.secId}:${q.subId}`)],
    [Markup.button.callback('К разделам', 'faq')],
    [Markup.button.callback('В меню', 'home')]
  ];
  return { text, markup: Markup.inlineKeyboard(rows), parse_mode: 'Markdown' };
}

// --------- Управление «живым» сообщением ---------
async function ensureMasterMessage(ctx, s) {
  if (s.master && s.master.chat_id && s.master.message_id) return s.master;
  const m = await ctx.reply('Загрузка меню…', kbMain());
  s.master = { chat_id: m.chat.id, message_id: m.message_id };
  return s.master;
}
async function renderHome(ctx, s) {
  const m = await ensureMasterMessage(ctx, s);
  const text = [
    '👋 Официальный бот Unity Auto.',
    'Проверим VIN, подберём авто из салона или рассчитаем импорт из 5 стран.',
    'Выберите действие ниже:'
  ].join('\n');
  await ctx.telegram.editMessageText(m.chat_id, m.message_id, undefined, text, kbMain());
}

async function deleteMasterIfAny(ctx, s) {
  if (s.master && s.master.chat_id && s.master.message_id) {
    try { await ctx.telegram.deleteMessage(s.master.chat_id, s.master.message_id); } catch (e) {}
    s.master = null;
  }
}
async function tryDeleteCurrentCallbackMessage(ctx, s) {
  const msg = ctx.callbackQuery && ctx.callbackQuery.message;
  if (!msg) return;
  if (!s.master || msg.message_id !== s.master.message_id) {
    try { await ctx.telegram.deleteMessage(msg.chat.id, msg.message_id); } catch (e) {}
  }
}
async function safeEdit(ctx, m, text, markup, parse_mode) {
  try {
    await ctx.telegram.editMessageText(m.chat_id, m.message_id, undefined, text, {
      ...markup,
      ...(parse_mode ? { parse_mode } : {}),
      disable_web_page_preview: true
    });
  } catch (e) {
    if (!String(e).includes('message is not modified')) {
      LOG.warn({ e: String(e) }, 'editMessageText warn');
    }
  }
}

// ----------------- /start -----------------
bot.start(async (ctx) => {
  try {
    const arg = (ctx.message.text.split(' ').slice(1)[0] || '').trim(); // deep-link
    const s = S.get(ctx.from.id) || {};
    s.id = ctx.from.id;
    s.start_param = arg || 'organic';
    s.utm = parseUTM(arg);
    s.flow = null;
    s.step = 0;
    s.lead_uid = ulid();
    S.set(ctx.from.id, s);

    // Опционально: видеокружок
    // try { await ctx.replyWithVideoNote({ source: path.join(__dirname, 'assets/welcome.mp4') }); } catch {}

    await renderHome(ctx, s);
  } catch (e) {
    LOG.error(e);
    await ctx.reply('Упс, что-то пошло не так. Попробуйте ещё раз.');
  }
});

// ----------------- Главные экшены -----------------
bot.action('home', async (ctx) => {
  await ctx.answerCbQuery();
  const s = S.get(ctx.from.id) || {};
  await tryDeleteCurrentCallbackMessage(ctx, s);
  s.flow = null;
  s.step = 0;
  await renderHome(ctx, s);
});

bot.action('faq', async (ctx) => {
  await ctx.answerCbQuery();
  const s = S.get(ctx.from.id) || {};
  await tryDeleteCurrentCallbackMessage(ctx, s);
  const m = await ensureMasterMessage(ctx, s);
  const { text, markup } = renderFaqSections();
  await safeEdit(ctx, m, text, markup);
});

bot.action(/^faq:sec:([^:]+)$/, async (ctx) => {
  await ctx.answerCbQuery();
  const s = S.get(ctx.from.id) || {};
  const m = await ensureMasterMessage(ctx, s);
  const secId = ctx.match[1];
  const { text, markup } = renderFaqSubs(secId);
  await safeEdit(ctx, m, text, markup);
});

bot.action(/^faq:sub:([^:]+):([^:]+)$/, async (ctx) => {
  await ctx.answerCbQuery();
  const s = S.get(ctx.from.id) || {};
  const m = await ensureMasterMessage(ctx, s);
  const secId = ctx.match[1], subId = ctx.match[2];
  const { text, markup } = renderFaqQuestions(secId, subId, 0);
  await safeEdit(ctx, m, text, markup);
});

bot.action(/^faq:list:([^:]+):([^:]+):?(\d+)?$/, async (ctx) => {
  await ctx.answerCbQuery();
  const s = S.get(ctx.from.id) || {};
  const m = await ensureMasterMessage(ctx, s);
  const secId = ctx.match[1], subId = ctx.match[2], page = Number(ctx.match[3] || 0);
  const { text, markup } = renderFaqQuestions(secId, subId, page);
  await safeEdit(ctx, m, text, markup);
});

bot.action(/^faq:q:(.+)$/, async (ctx) => {
  await ctx.answerCbQuery();
  const s = S.get(ctx.from.id) || {};
  const m = await ensureMasterMessage(ctx, s);
  const qid = ctx.match[1];
  const { text, markup, parse_mode } = renderFaqAnswer(qid);
  await safeEdit(ctx, m, text, markup, parse_mode);
});

bot.action('flow:import', async (ctx) => {
  await ctx.answerCbQuery();
  const s = S.get(ctx.from.id) || {};
  await tryDeleteCurrentCallbackMessage(ctx, s);

  s.flow = 'import';
  s.step = 1;
  s.country = null;
  s.delivery_city = null;
  s.budget = null;
  s.prefs = {};
  S.set(ctx.from.id, s);
  const m = await ensureMasterMessage(ctx, s);
  const { text, markup } = renderImportPage(s);
  await ctx.telegram.editMessageText(m.chat_id, m.message_id, undefined, text, markup);
});

bot.action('flow:salon', async (ctx) => {
  await ctx.answerCbQuery();
  const s = S.get(ctx.from.id) || {};
  await tryDeleteCurrentCallbackMessage(ctx, s);

  s.flow = 'salon';
  s.step = 1;
  s.city = null;
  s.budget = null;
  s.prefs = {};
  S.set(ctx.from.id, s);
  const m = await ensureMasterMessage(ctx, s);
  const { text, markup } = renderSalonPage(s);
  await ctx.telegram.editMessageText(m.chat_id, m.message_id, undefined, text, markup);
});

// --------- Импорт: шаги ---------
bot.action(/imp:(.+)/, async (ctx) => {
  await ctx.answerCbQuery();
  const s = S.get(ctx.from.id); if (!s) return;

  const [type, a, b] = ctx.match[1].split(':'); // country:KR / dc:msk / bud:a / pref:body:седан / cm:tg / next / back / contact
  if (type === 'country') { s.country = a; s.step = 2; }
  if (type === 'dc') {
    const found = PRESET_CITIES.find(x => x.k === a);
    if (found) {
      if (a === 'oth') s.await_text = 'delivery_city';
      else { s.delivery_city = found.t; s.step = 3; }
    }
  }
  if (type === 'bud') {
    const preset = PRESET_BUDGETS.find(x => x.k === a);
    if (preset) {
      if (preset.custom) s.await_text = 'budget_custom';
      else { s.budget = { min: preset.min, max: preset.max, currency: 'RUB' }; s.step = 4; }
    }
  }
  if (type === 'pref') {
    const [field, val] = [a, b];
    s.prefs = s.prefs || {};
    s.prefs[field] = (s.prefs[field] === val) ? null : val;
  }
  if (type === 'cm') { s.contact_method = a; }
  if (type === 'next') { s.step = Math.min(6, (s.step || 1) + 1); }
  if (type === 'back') { s.step = Math.max(1, (s.step || 1) - 1); }
  if (type === 'contact') { await askContact(ctx, s); }

  const m = await ensureMasterMessage(ctx, s);
  const { text, markup } = renderImportPage(s);
  await safeEdit(ctx, m, text, markup);
});

// --------- Салон: шаги ---------
bot.action(/sal:(.+)/, async (ctx) => {
  await ctx.answerCbQuery();
  const s = S.get(ctx.from.id); if (!s) return;

  const [type, a, b] = ctx.match[1].split(':'); // city:msk / bud:a / pref:body:седан / cm:tg / next / back / contact
  if (type === 'city') {
    const found = PRESET_CITIES.find(x => x.k === a);
    if (found) {
      if (a === 'oth') s.await_text = 'city';
      else { s.city = found.t; s.step = 2; }
    }
  }
  if (type === 'bud') {
    const preset = PRESET_BUDGETS.find(x => x.k === a);
    if (preset) {
      if (preset.custom) s.await_text = 'budget_custom';
      else { s.budget = { min: preset.min, max: preset.max, currency: 'RUB' }; s.step = 3; }
    }
  }
  if (type === 'pref') {
    const [field, val] = [a, b];
    s.prefs = s.prefs || {};
    s.prefs[field] = (s.prefs[field] === val) ? null : val;
  }
  if (type === 'cm') { s.contact_method = a; }
  if (type === 'next') { s.step = Math.min(5, (s.step || 1) + 1); }
  if (type === 'back') { s.step = Math.max(1, (s.step || 1) - 1); }
  if (type === 'contact') { await askContact(ctx, s); }

  const m = await ensureMasterMessage(ctx, s);
  const { text, markup } = renderSalonPage(s);
  await safeEdit(ctx, m, text, markup);
});

// --------- Приём контакта ---------
bot.on('contact', async (ctx) => {
  const s = S.get(ctx.from.id);
  if (!s) return;

  const phone = normalizePhoneE164(ctx.message.contact.phone_number);
  s.phone = phone;
  s.contact_name = `${ctx.message.contact.first_name || ''} ${ctx.message.contact.last_name || ''}`.trim() || ctx.from.first_name || '';

  // (1) Подтверждение + убираем клавиатуру
  await ctx.reply('Спасибо! Контакт получен ✅', Markup.removeKeyboard());

  // (2) Удаляем верхнее «живое» сообщение мастера
  await deleteMasterIfAny(ctx, s);

  // (3) Финализируем и шлём финал новым сообщением
  await finalizeAndSend(ctx, s, { sendNew: true });
});

// --------- Приём обычного текста (города/бюджет custom) ---------
bot.on('text', async (ctx) => {
  const s = S.get(ctx.from.id);
  if (!s || !s.await_text) return;
  const t = ctx.message.text.trim();

  if (s.await_text === 'delivery_city') {
    s.delivery_city = t; s.await_text = null; s.step = 3;
  } else if (s.await_text === 'city') {
    s.city = t; s.await_text = null; s.step = 2;
  } else if (s.await_text === 'budget_custom') {
    const num = Number(t.replace(/\s+/g, '').replace(/[^\d]/g, ''));
    if (Number.isFinite(num) && num > 0) {
      s.budget = { min: num, max: null, currency: 'RUB' };
      s.await_text = null; s.step = s.flow === 'import' ? 4 : 3;
    } else {
      await ctx.reply('Введите сумму числом, например: 2000000');
      return;
    }
  }

  const m = await ensureMasterMessage(ctx, s);
  const view = s.flow === 'import' ? renderImportPage(s) : renderSalonPage(s);
  await safeEdit(ctx, m, view.text, view.markup);
});

// --------- Финализация и отправка в amoCRM ---------
async function finalizeAndSend(ctx, s, { sendNew = false } = {}) {
  try {
    if (!s.phone) {
      await ctx.reply('Нужно подтвердить телефон, нажмите «Отправить телефон».');
      return;
    }

    // дедуп (30 дней) — бот-сторона
    const last = dedup[s.phone];
    if (last && dayjs().diff(dayjs(last), 'day') < 30) {
      LOG.info({ phone: s.phone }, 'dedup: recent');
    }
    dedup[s.phone] = nowISO(); saveJSON(DEDUP_PATH, dedup);

    const payload = buildPayloadFromState(ctx, s);
    const responsible_id = nextResponsible();
    queueAmoDelivery({ payload, responsible_id });

    const text = [
      '✅ Заявка отправлена! Эксперт свяжется в ближайшее время (обычно до 15 минут).',
      'Хотите оформить ещё один расчёт или вернуться в меню?'
    ].join('\n');
    const kb = Markup.inlineKeyboard([
      [Markup.button.callback('Ещё один расчёт', 'flow:import')],
      [Markup.button.callback('В меню', 'home')],
      [Markup.button.callback('Ответы на вопросы', 'faq')]
    ]);

    if (sendNew) await ctx.reply(text, kb);
    else {
      const m = await ensureMasterMessage(ctx, s);
      await safeEdit(ctx, m, text, kb);
    }

    s.step = 0;
  } catch (e) {
    LOG.error(e, 'finalize error');
    await ctx.reply('Не получилось отправить заявку сразу. Мы повторим попытку автоматически.');
  }
}

function buildPayloadFromState(ctx, s) {
  return {
    lead_uid: s.lead_uid || ulid(),
    created_at: nowISO(),
    flow: s.flow, // import | salon
    source: { start_param: s.start_param || 'organic', utm: s.utm || {} },
    contact: {
      phone_e164: s.phone,
      tg_username: ctx.from.username || '',
      name: s.contact_name || `${ctx.from.first_name || ''} ${ctx.from.last_name || ''}`.trim()
    },
    geo: {
      city: s.city || null,
      delivery_city: s.delivery_city || null
    },
    budget: s.budget || null,
    prefs: {
      country: s.country || null,
      body: s.prefs?.body || null,
      fuel: s.prefs?.fuel || null,
      transmission: s.prefs?.transmission || null,
      drive: s.prefs?.drive || null
    },
    consent: {
      accepted: true,
      version: '2025-09-01',
      ts: nowISO()
    }
  };
}

// ----------------- Очередь доставки в amoCRM -----------------
function queueAmoDelivery({ payload, responsible_id }) {
  outbox.push({
    id: ulid(),
    t: 'amo.lead',
    payload,
    responsible_id: responsible_id || null,
    attempts: 0,
    next_at: Date.now()
  });
  saveJSON(OUTBOX_PATH, outbox);
}

setInterval(processOutboxTick, 2000);

async function processOutboxTick() {
  const now = Date.now();
  for (const job of [...outbox]) {
    if (job.next_at > now) continue;
    try {
      if (job.t === 'amo.lead') {
        await deliverLeadToAmo(job.payload, job.responsible_id);
      }
      const idx = outbox.findIndex(x => x.id === job.id);
      if (idx >= 0) outbox.splice(idx, 1);
      saveJSON(OUTBOX_PATH, outbox);
    } catch (e) {
      job.attempts += 1;
      const backoffMs = attemptBackoff(job.attempts);
      job.next_at = Date.now() + backoffMs;
      saveJSON(OUTBOX_PATH, outbox);
      LOG.error({ err: String(e), attempts: job.attempts }, 'Outbox retry scheduled');
      if (job.attempts === 3 && ADMIN_CHAT_ID) {
        try { await bot.telegram.sendMessage(ADMIN_CHAT_ID, `⚠️ amoCRM недоступна, повторная попытка через ${Math.round(backoffMs/1000)}с`); } catch {}
      }
    }
  }
}

function attemptBackoff(n) {
  if (n <= 1) return 60 * 1000;        // 1 мин
  if (n === 2) return 5 * 60 * 1000;   // 5 мин
  if (n === 3) return 15 * 60 * 1000;  // 15 мин
  if (n === 4) return 60 * 60 * 1000;  // 1 ч
  return 6 * 60 * 60 * 1000;           // 6 ч
}

async function deliverLeadToAmo(payload, responsible_id) {
  const { contact, flow, prefs, source, geo, budget, consent } = payload;

  // 1) Контакт
  const contact_id = await amo.createOrUpdateContact({
    phone_e164: contact.phone_e164,
    name: contact.name,
    tg_username: contact.tg_username
  });

  // 2) Сделка
  const lead_id = await amo.createLead({
    flow,
    country: prefs.country,
    delivery_city: geo.delivery_city,
    city: geo.city,
    budget,
    prefs,
    start_param: source.start_param,
    utm: source.utm,
    consent,
    responsible_id
  });

  // 3) Линковка
  await amo.linkContactToLead(contact_id, lead_id);

  // 4) Задача + заметка
  const completeAt = Date.now() + 15 * 60 * 1000;
  await amo.addTask(lead_id, 'Первичный контакт с клиентом', completeAt);
  await amo.addNote(lead_id, leadSummary(payload));

  LOG.info({ lead_id, contact_id }, 'amo: lead created');
}

function leadSummary(p) {
  const parts = [];
  if (p.flow === 'import') {
    parts.push(`Импорт: ${p.prefs.country || '-'}`);
    parts.push(`Доставка: ${p.geo.delivery_city || '-'}`);
  } else {
    parts.push(`Салон: ${p.geo.city || '-'}`);
  }
  parts.push(`Бюджет: ${humanBudget(p.budget)}`);
  if (p.prefs) {
    if (p.prefs.body) parts.push(`Кузов: ${p.prefs.body}`);
    if (p.prefs.transmission) parts.push(`КПП: ${p.prefs.transmission}`);
    if (p.prefs.drive) parts.push(`Привод: ${p.prefs.drive}`);
    if (p.prefs.fuel) parts.push(`Топливо: ${p.prefs.fuel}`);
  }
  parts.push(`Источник: ${p.source.start_param || '—'}`);
  return parts.join('\n');
}

// --------- Запрос телефона (ПДн) ---------
async function askContact(ctx, s) {
  await ctx.reply(
    'Нажимая кнопку ниже, вы соглашаетесь на обработку персональных данных и обратную связь. Политика: https://example.com/privacy',
    Markup.keyboard([Markup.button.contactRequest('Отправить мой телефон')]).oneTime().resize()
  );
}

// ----------------- Запуск -----------------
bot.launch().then(() => {
  LOG.info('LeadBot started');
}).catch(e => {
  LOG.error(e, 'bot.launch error');
  process.exit(1);
});

// Корректное завершение
process.once('SIGINT', () => bot.stop('SIGINT'));
process.once('SIGTERM', () => bot.stop('SIGTERM'));
