// messages.js — MarkdownV2 безопасные тексты и хелперы

const LINKS = {
  BOT: process.env.FREE_RF_BOT_URL || 'https://t.me/Unity_Auto_bot',
  CATALOG: process.env.FREE_RF_CATALOG_URL || 'https://t.me/autotradercars',
  CHANNEL: process.env.FREE_RF_SUBCHANNEL_URL || 'https://t.me/autonews_unity',
};

// Хелперы Markdown-V2
const mdv2 = {
  // Экранируем все служебные символы Markdown-V2
  esc: (s = '') => String(s).replace(/([_*\[\]()~`>#+\-=|{}.!\\])/g, '\\$1'),
  b: (s = '') => `*${mdv2.esc(s)}*`,
  i: (s = '') => `_${mdv2.esc(s)}_`,
  code: (s = '') => '`' + String(s).replace(/[`\\]/g, '\\$&') + '`',
  link: (label, url) => `[${mdv2.esc(label)}](${url})`, // URL не экранируем
};

// -------- Тексты (используются в index.js) --------
const MSG_CALC_INTRO =
  mdv2.b('Калькулятор: доставка & растаможка') +
  '\n\n' +
  mdv2.esc('Давайте рассчитаем наилучшую стоимость доставки и растаможки автомобиля из') +
  ' ' +
  mdv2.b('Европы, Китая, Кореи, США и ОАЭ') +
  ' 👇';

const MSG_QA_INTRO =
  '💡 ' + mdv2.i('Пожалуйста, ответьте на несколько вопросов ниже.') + '\n' +
  mdv2.esc('Это поможет мне сформировать наиболее точный ответ, который будет удовлетворять вашему запросу.') + '\n' +
  '🔽';

const MSG_ASK_MAKE_MODEL =
  '🚗 ' + mdv2.b('Напишите МАРКУ и МОДЕЛЬ автомобиля.') + '\n' +
  mdv2.i('Используйте латинский алфавит, разделяя марку и модель запятой.') + '\n' +
  '_' + mdv2.esc('[Пример: ') + mdv2.code('Mercedes G500') + mdv2.esc(']') + '_';

const MSG_ASK_CONDITION =
  '💬 ' +
  mdv2.esc('Какой автомобиль вас интересует:') + ' ' +
  mdv2.b('новый') + ' ' + mdv2.esc('/') + ' ' + mdv2.b('с пробегом') + '?';

const MSG_CUSTOMS_NOTE = (car, city) =>
  '🧾 ' + mdv2.esc('Стоимость растаможки зависит от конкретного автомобиля.') + '\n' +
  mdv2.esc('Давайте подберём для вас') + ' ' + mdv2.b(car) + ' ' +
  mdv2.esc('и отправим готовое предложение «под ключ» в') + ' ' + mdv2.b(city) + '.';

const MSG_THANKS_SERVICES = [
  mdv2.esc('Спасибо за обращение! Ожидайте получения вашего персонального предложения.'),
  mdv2.esc('Предлагаем ознакомиться с нашими полезными сервисами.'),
  '',
  '- ' + mdv2.link('AUTO NEWS', LINKS.CHANNEL) + ' — ' + mdv2.esc('Будьте в курсе последних автоновостей России и мира.'),
  '- ' + mdv2.link('Telegram КАТАЛОГ', LINKS.CATALOG) + ' — ' + mdv2.esc('Горячие предложения по автомобилям в наличии в автосалоне UNITY.'),
  '- ' + mdv2.link('VIN_BOT', LINKS.BOT) + ' — ' + mdv2.esc('Бесплатная проверка истории авто по VIN-коду.'),
].join('\n');

module.exports = {
  LINKS,
  mdv2,
  MSG_CALC_INTRO,
  MSG_QA_INTRO,
  MSG_ASK_MAKE_MODEL,
  MSG_ASK_CONDITION,
  MSG_CUSTOMS_NOTE,
  MSG_THANKS_SERVICES,
};
