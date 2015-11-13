package org.zirbes.rxjava

import groovy.transform.CompileStatic

import org.joda.time.LocalDateTime

@CompileStatic
class Bird {

    Bird(String type, String name) {
        this.type = type
        this.name = name
    }

    Bird(int index) {
        this.number = index
        this.type = BirdType.type(index)
        this.name = BirdType.name(index)
    }

    int number

    String type

    String name

    LocalDateTime time = LocalDateTime.now()

    String toString() {
        return "${type} #${number} named ${name} @ ${time}"
    }

}
