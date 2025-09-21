const LINKS = {
  BOT: process.env.FREE_RF_BOT_URL || 'https://t.me/Unity_Auto_bot',
  CATALOG: process.env.FREE_RF_CATALOG_URL || 'https://t.me/autotradercars',   
  CHANNEL: process.env.FREE_RF_SUBCHANNEL_URL ||'https://t.me/autonews_unity',   
};
const MSG_CALC_INTRO = `<b>Калькулятор: доставка &amp; растаможка</b>

Давайте рассчитаем наилучшую стоимость доставки и растаможки автомобиля из <b>Европы, Китая, Кореи, США и ОАЭ</b> 👇`;

const MSG_QA_INTRO = `💡 <i>Пожалуйста, ответьте на несколько вопросов ниже.</i>
Это поможет мне сформировать наиболее точный ответ, который будет удовлетворять вашему запросу.
🔽`;

const MSG_ASK_MAKE_MODEL = `🚗 <b>Напишите МАРКУ и МОДЕЛЬ автомобиля.</b>
<i>Используйте латинский алфавит, разделяя марку и модель запятой.</i>
<i>[Пример: Mercedes, g500]</i>`;

const MSG_ASK_CONDITION = `💬 Какой автомобиль вас интересует: <b>новый</b> / <b>с пробегом</b>?`;

const MSG_CUSTOMS_NOTE = `🧾 Стоимость растаможки зависит от конкретного автомобиля.
Давайте подберём для вас <b>{{CAR}}</b> и отправим готовое предложение «под ключ» в <b>{{CITY}}</b>.`;

const MSG_THANKS_SERVICES = `Спасибо за обращение! Ожидайте получения вашего персонального предложения.
Предлагаем ознакомиться с нашими полезными сервисами.

- <a href="${LINKS.CHANNEL}">AUTO NEWS</a> | — Будьте в курсе последних автоновостей России и мира.
- <a href="${LINKS.CATALOG}">Telegram КАТАЛОГ</a> | — Горячие предложения по автомобилям в наличии в автосалоне UNITY.
- <a href="${LINKS.BOT}">VIN_BOT</a> | — Бесплатная проверка истории авто по VIN-коду.`;

// Экспорт для использования в боте
module.exports = {
  MSG_CALC_INTRO,
  MSG_QA_INTRO,
  MSG_ASK_MAKE_MODEL,
  MSG_ASK_CONDITION,
  MSG_CUSTOMS_NOTE,
  MSG_THANKS_SERVICES,
};