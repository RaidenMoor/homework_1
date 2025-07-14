package org.example.service;

import org.json.JSONObject;

import java.util.Random;

public class WeatherService {

    public WeatherService(){

    }

    public JSONObject GenerateMessage(){
        Random random = new Random();
        // Генерируем случайные данные о погоде
        int temperature = random.nextInt(36); // Температура от 0 до 35
        String[] conditions = {"солнечно", "облачно", "дождь"};
        String condition = conditions[random.nextInt(conditions.length)];

        // Создаем JSON объект
        JSONObject weatherData = new JSONObject();
        weatherData.put("temperature", temperature);
        weatherData.put("condition", condition);

        return weatherData;
    }
}
