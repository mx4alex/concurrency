package main

import (
	"fmt"
	"sync"
	"sort"
	"strconv"
)

func RunPipeline(cmds ...cmd) {
	in := make(chan interface{})

	for _, command := range cmds {
		out := make(chan interface{})
		  
		go func(c cmd, in, out chan interface{}) {
			defer close(out)
			c(in, out)
		}(command, in, out)
		
		in = out
	}

	<-in
}

func SelectUsers(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	users := make(map[string]struct{})

	for email := range in {
		inputEmail, ok := email.(string)
		if !ok {
			continue
		}

		wg.Add(1)
		go func(inputEmail string) {
			defer wg.Done()
			user := GetUser(inputEmail)
			email := user.Email
			
			mu.Lock()
			defer mu.Unlock()
			_, ok := users[email]
			if !ok {
				out <- user
				users[email] = struct{}{}
			}
		}(inputEmail)
	}

	wg.Wait()
}

func outMessages(out chan interface{}, users ...User) {
	ids, err := GetMessages(users...)
	if err != nil {
		fmt.Printf("Error in GetMessages: %v \n", err)
	}

	for _, id := range ids {
		out <- id
	}
}

func SelectMessages(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	users := make([]User, 0, GetMessagesMaxUsersBatch)

	for user := range in {
		inputUser, ok := user.(User) 
		if !ok {
			continue
		}

	 	users = append(users, inputUser)

		if len(users) == GetMessagesMaxUsersBatch {
			wg.Add(1)
			copyUsers := make([]User, GetMessagesMaxUsersBatch)
			copy(copyUsers, users)

			go func(inputUsers ...User) {
				defer wg.Done()
				outMessages(out, inputUsers...)
			}(copyUsers...)
	
			users = users[:0]
		}
	}

	if len(users) != 0 {
		outMessages(out, users...)
	} 

	wg.Wait()
}

func CheckSpam(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	workerInput := make(chan MsgID)

	for i := 0 ; i < HasSpamMaxAsyncRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range workerInput{
				isSpam, err := HasSpam(id)
				if err != nil {
					fmt.Printf("Error in HasSpam: %v", err)
				}

				result := MsgData{
					ID:      id,
					HasSpam: isSpam,
				}

				out <- result
			}
		}()
	}

	for id := range in {
		inputID, ok := id.(MsgID)
		if !ok {
			continue
		}

		workerInput <- inputID
	}

	close(workerInput)
	wg.Wait()
}

type Data []MsgData

func (d Data) Len() int           { return len(d) }
func (d Data) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }
func (d Data) Less(i, j int) bool { 
	if d[i].HasSpam == d[j].HasSpam { 
		return d[i].ID < d[j].ID 
	}
	return d[i].HasSpam
}

func CombineResults(in, out chan interface{}) {
	msgData := Data{}
	var result string

	for data := range in {
		inputData, ok := data.(MsgData)
		if !ok {
			continue
		}

		msgData = append(msgData, inputData)
	}

	sort.Sort(msgData)

	for _, data := range msgData {
		if data.HasSpam {
			result = "true " + strconv.FormatUint(uint64(data.ID), 10)
		} else {
			result = "false " + strconv.FormatUint(uint64(data.ID), 10)
		}

		out <- result
	}
}