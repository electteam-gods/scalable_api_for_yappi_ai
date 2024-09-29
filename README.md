# <p align="center"> ЦИФРОВОЙ ПРОРЫВ: СЕЗОН ИИ </p>
# <p align="center"> Выравнивание изображений номеров автомобилей </p>

# <p align="center"> ЭТО РЕПОЗИТОРИЙ С РЕАЛИЗАЦИЕЙ API И ДЕМОНСТРАЦИОННОГО ВЕБ-ПРИЛОЖЕНИЯ, НЕЙРОННАЯ МОДЕЛЬ НАХОДИТСЯ [В ЭТОМ РЕПОЗИТОРИИ](https://github.com/electteam-gods/model) </p>

<p align="center">
<img width="400" height="400" alt="photo" src="https://github.com/NikitaGordievskiy/align-car-plates-beeline/blob/b9c650acbbf7d6690ae649edc2df5506344327f7/logo_electteam.jpg">
</p> 

## Оглавление
1. [Задание](#1)
3. [Запуск](#12)
4. [Решение](#2)
5. [Практическая применимость](#3)
6. [Уникальность нашего решения](#4)
7. [Планы развития](#5)
8. [Стек](#6)
9. [Полезные ссылки](#7)

## <a name="1"> Задание </a>

Наша команда разработала программный модуль, являющийся частью решения задачи видеоаналитики транспорта, позволяющий выровнять вырезанные из видеопотока изображения номеров автомобиля.

Проблематика решения состоит в том, что в задаче видеоаналитики корректность определения каждого номера является одним из самых важных показателей, и для бизнеса цена ошибки распознавания может стать критичной, поэтому необходимо стремиться разработать такие решения, которые смогут выдать абсолютно точные показатели.

Одним из этапов достижения поставленной цели является выравнивание перспективы вырезанных из видеопотока автомобильных номеров.

## <a name="12"> Запуск </a>

Модель была предварительно упакована в Docker-образ и доступна на Docker Hub: `docker.io/oleg36913/lcar_plate_app:v3`

Решение оформленно в виде монорепы, все сервисы поднимаются при помощи Docker Compose файлов:
1. `docker.compose.dev.prd.yml` - для прода
1. `docker.compose.dev.yml` - для разработки

По шагам:
1. `git clone ...` - клонируем репозиторий
2. `cd ...` - переходим в папку с репозиторием
3. `docker compose up -f docker.compose.dev.prd.yml up -d` - запускаем весь стек
4. Ждём пару минут...
5. `echo "Enjoy!"`

S3 бакет был предоставлен сторонним облачным провайдером в целях экономии времени, но ничего не мешает поставить MinIO и организовать локальное S3 хранилище.

Проект без труда скейлится горизонтально, воркер ноды никакого внутреннего состояния не имеют.

### Пример решения задачи оцифровки номера автомобиля
<img width="1300" height="280" alt="image" src="https://github.com/NikitaGordievskiy/align-car-plates-beeline/blob/67581fb864aab7405ee32cd875c0931103288471/task_example.png"> 

## <a name="2">Решение </a>

Для получения качественного решения задачи выравнивания необходимо пройти через два этапа:

1) Детекция таблички номера. В процессе решения нами были рассмотрены несколько моделей и конечный выбор пал именно на модель поиска угловых точек номера автомобиля с помощью ultralytics YoloV8, потому что, по сравнению с рассмотренными нами другими решениями, она показывает более точные результаты.
2) Применение перспективного матричного преобразования при помощи методов библиотеки OpenCV на основе этих самых точек. Такое матричное преобразование использовалось, поскольку данный подход позволяет получить как хорошее качество выравнивания перспективы при небольших затратах, так и скоростное решение. Как итог, мы получили, что одно изображение обрабатывается в среднем около 100 миллисекунд.

### Архетиктура решения

<img width="1200" height="300" alt="image" src="https://github.com/NikitaGordievskiy/align-car-plates-beeline/blob/aa16786319ecbef7a63e088f4b5e95736e0d9a30/model_scheme.png"> 

## <a name="3">Практическая применимость </a>

Практическая применимость решения заключается в простой интеграции нашего решения в большую задачу.

У нас обработка запроса происходит в 5 этапов:

1. Отправка "клиентом" запроса с ссылкой на изображение, которое предварительно было загружено на любое файловое хранилище, например S3-бакет.
2. Слой API создаёт задачу обработки фотографии и отправляет ее в очередь NEW_JOBS.
3. Worker'ы вытягивают из очереди NEW_JOBS новые задачи и начинают их обработку.
4. Завершив обработку Worker отправляет сообщение с результатом обработки номерного знака в очередь READY_JOBS.
5. API отдает клиенту выровненный под трафарет номер.

### Архетиктура системы

<img width="1200" height="300" alt="image" src="https://github.com/NikitaGordievskiy/align-car-plates-beeline/blob/b2538e940c6b62e93547cb9fd7e096e900598efd/system_scheme.png"> 

## <a name="4">Уникальность нашего решения </a>

Горизонтальное масштабирование. Worker ноды не обладают состоянием, благодаря чему легко масштабируются горизонтально. Система выдерживает большие нагрузки.

Мы не всегда знаем, какого рода видеопоток у нас рассматривается: запись с камер видеонаблюдения или шлагбаум. Для этого масштабируемость и применяется, что можно было бы рассматривать задачу анализа различного видеопотока, как единое целое. Нам все равно приходит изображение так сказать пачками, если это вырезано с камеры видеонаблюдения, либо же оно приходит последовательно. У бизнеса могут быть разные задачи, но наше решение готово решить любую поставленную задачу.
 
Используемый подход решения обеспечивает универсальность метода, поскольку мы не опираемся на конкретный шаблон, а значит мы можем работать с номерами любого формата (однострочного, двустрочного).

Также можем работать с номерами любого из пяти установленных цветов номерных знаков в Российской Федерации. Это осуществляется при помощи методов библиотеки OpenCV, переводящее изображение в черно/белый формат.

## <a name="5">Планы развития </a>

1 Дообучение моделей на большем количестве данных.

2 Повышение точности попадания в трафарет.

3 Повышение точности распознавания за счет улучшения качества фотографий.

4 Повышение быстродействия и масштабируемости.

## <a name="6">Стек </a>

<img src="https://github.com/devicons/devicon/blob/master/icons/python/python-original-wordmark.svg" title="Python" alt="Puthon" width="40" height="40"/>&nbsp;
<img src="https://github.com/devicons/devicon/blob/6910f0503efdd315c8f9b858234310c06e04d9c0/icons/docker/docker-plain-wordmark.svg" title="Docker" alt="Puthon" width="40" height="40"/>&nbsp;
  <img src="https://github.com/devicons/devicon/blob/master/icons/pytorch/pytorch-original.svg" title="Pytorch" alt="Puthon" width="40" height="40"/>&nbsp;
  <img src="https://github.com/devicons/devicon/blob/master/icons/numpy/numpy-original.svg" title="Numpy" alt="Puthon" width="40" height="40"/>&nbsp;
  <img src="https://github.com/devicons/devicon/blob/master/icons/opencv/opencv-original.svg" title="OpenCV" alt="Puthon" width="40" height="40"/>&nbsp;
  <img src="https://github.com/devicons/devicon/blob/6910f0503efdd315c8f9b858234310c06e04d9c0/icons/vuejs/vuejs-original-wordmark.svg" title="Vue" alt="Puthon" width="40" height="40"/>&nbsp;
  <img src="https://github.com/devicons/devicon/blob/6910f0503efdd315c8f9b858234310c06e04d9c0/icons/typescript/typescript-original.svg" title="TypeScript" alt="Puthon" width="40" height="40"/>&nbsp;
  <img src="https://github.com/devicons/devicon/blob/6910f0503efdd315c8f9b858234310c06e04d9c0/icons/rabbitmq/rabbitmq-original-wordmark.svg" title="RabbitMQ" alt="Puthon" width="40" height="40"/>&nbsp;
  <img src="https://github.com/devicons/devicon/blob/6910f0503efdd315c8f9b858234310c06e04d9c0/icons/redis/redis-original-wordmark.svg" title="Redis" alt="Puthon" width="40" height="40"/>&nbsp;
<img src="https://cdn.prod.website-files.com/646dd1f1a3703e451ba81ecc/6499468f33db295c5a1219ec_Ultralytics_mark_blue.svg" title="Ultralytics" alt="Puthon" width="40" height="40"/>&nbsp;

## <a name="7">Полезные ссылки </a>

- [ссылка на веса модели детекции](https://github.com/electteam-gods/model/tree/main/weights)
- [ссылка на код модели с комментариями](https://github.com/electteam-gods/model)
- [ссылка на код формирования таблицы ответов на тестовый датасет](https://github.com/electteam-gods/model/blob/main/pipeline.ipynb)
- [ссылка на скринкаст](https://disk.yandex.ru/d/lcg9v136wceEGw)&nbsp;
- [ссылка на демо](http://plates.ellecteam.ayarayarovich.ru)&nbsp;
- [ссылка на код обучения](https://github.com/electteam-gods/model/blob/main/train-notbook.ipynb)
